#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <sys/epoll.h>
#include <signal.h>
#include <math.h>
#include <time.h>
#include "hashtable.h"
#include "zset.h"
#include "common.h"
#include "list.h"
#include "heap.h"

#define MAX_EVENTS 20
#define PORT 8085

volatile sig_atomic_t stop = 0;

void handle_signal(int sig) {
    stop = 1;
}

static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

static void fd_set_nb(int fd) {
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }
}

const size_t k_max_msg = 4096;
const size_t k_max_args = 1024;

enum {
    STATE_REQ = 0,
    STATE_RES = 1,
    STATE_END = 2,  // mark the connection for deletion
};

enum {
    RES_OK = 0,
    RES_ERR = 1,
    RES_NX = 2,
};

enum {
    ERR_UNKNOWN = 1, // unknow error
    ERR_2BIG = 2,    // msg too long
    ERR_TYPE = 3,    // data type
    ERR_ARG = 4,    // ivaliad argument
};


static void out_nil(std::string &out){
    out.push_back(SER_NIL);
}

static void out_err(std::string &out,  int32_t code, const std::string &msg){
    out.push_back(SER_ERR);
    out.append((char *)&code, 4);
    uint32_t len = (uint32_t)msg.size();
    out.append((char *)&len, 4);
    out.append(msg);
}

static void out_kv(std::string &out, const std::string &key,const std::string &val){
    out.push_back(SER_KV);
    uint32_t total_len = key.size() + val.size() + 2 * sizeof(uint32_t);
    out.append((char *)&total_len, 4);
     // Add key length and key
    uint32_t key_len = (uint32_t)key.size();
    out.append((char *)&key_len, sizeof(key_len)); // Append key length
    out.append(key);                               // Append key data

    // Add value length and value
    uint32_t val_len = (uint32_t)val.size();
    out.append((char *)&val_len, sizeof(val_len)); // Append value length
    out.append(val);                               // Append value data
} 

static void out_str(std::string &out, const std::string &val){
    out.push_back(SER_STR);
    uint32_t len = (uint32_t)val.size();
    out.append((char *)&len, 4);
    out.append(val);
} 

static void out_int(std::string &out, int64_t val) {
    out.push_back(SER_INT);
    out.append((char *)&val, 8);
}

static void out_arr(std::string &out, uint32_t n) {
    out.push_back(SER_ARR);
    out.append((char *)&n, 4);
}

static void out_dbl(std::string &out, double val) {
    out.push_back(SER_DBL);
    out.append((char *)&val, 8);

}

static void out_str(std::string &out, const char *s, size_t size) {
    out.push_back(SER_STR);
    uint32_t len = (uint32_t)size;
    out.append((char *)&len, 4);
    out.append(s, len);
}

static void *begin_arr(std::string &out) {
    out.push_back(SER_ARR);
    out.append("\0\0\0\0", 4);          // filled in end_arr()
    return (void *)(out.size() - 4);    // the `ctx` arg
}

static void end_arr(std::string &out, void *ctx, uint32_t n) {
    size_t pos = (size_t)ctx;
    assert(out[pos - 1] == SER_ARR);
    memcpy(&out[pos], &n, 4);
}

enum {
    T_STR = 0,
    T_ZSET = 1,
};

struct Entry {
	HNode node;
	std::string key;
	std::string value;
    uint32_t type = 0;
    ZSet* zset = NULL;
    size_t heap_idx = -1;
};

static uint64_t get_monotonic_usec(){
    timespec tv  ={0, 0};
    clock_gettime(CLOCK_MONOTONIC,&tv);
    return tv.tv_sec * 1000000 + tv.tv_nsec / 1000;
} 

struct Conn {
    int fd = -1;
    uint32_t state = 0;     // either STATE_REQ or STATE_RES
    // buffer for reading
    size_t rbuf_size = 0;
    uint8_t rbuf[4 + k_max_msg];
    // buffer for writing
    size_t wbuf_size = 0;
    size_t wbuf_sent = 0;
    uint8_t wbuf[4 + k_max_msg];
    uint64_t idle_start = 0;
    DList idle_list;
};


static struct{
    HMap db;
    // a map of all client connections, keyed by fd
    std::vector<Conn *> fd2conn;
    DList idle_list;
    std::vector<HeapItem> heap;
}g_data; 


bool entry_eq(HNode* lhs, HNode* rhs){
    struct Entry *le = container_of(lhs, struct Entry,node);
    struct Entry *re = container_of(lhs, struct Entry,node);
    return le->key == re->key;
} 

static void conn_put(std::vector<Conn *> &fd2conn, struct Conn *conn) {
    if (fd2conn.size() <= (size_t)conn->fd) {
        fd2conn.resize(conn->fd + 1);
    }
    fd2conn[conn->fd] = conn;
}

static int32_t accept_new_conn(std::vector<Conn *> &fd2conn, int epfd, int fd) {
    struct sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);

    while (true) {
        int connfd = accept(fd, (struct sockaddr *)&client_addr, &socklen);
        if (connfd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            perror("accept");
            return -1;
        }

        fd_set_nb(connfd);

        Conn *conn = (Conn *)malloc(sizeof(Conn));
        if (!conn) {
            close(connfd);
            continue;
        }

        conn->fd = connfd;
        conn->state = STATE_REQ;
        conn->rbuf_size = 0;
        conn->wbuf_size = 0;
        conn->wbuf_sent = 0;
        conn->idle_start = get_monotonic_usec();
        dlist_insert_before(&g_data.idle_list, &conn->idle_list);
        conn_put(fd2conn, conn);

        struct epoll_event event = {};
        event.data.fd = connfd;
        event.events = EPOLLIN | EPOLLET;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &event) < 0) {
            perror("epoll_ctl");
            return -1;
        }
    }

    return 0;
}


static void state_req(Conn *conn);
static void state_res(Conn *conn);

static int32_t parse_req(const uint8_t *data, uint32_t reqlen,std::vector<std::string> &out ){

    if (reqlen < 4){
        return -1;
    } 
    
    uint32_t n = 0; 
    memcpy(&n,&data[0],4); // try to get the number of arguments

    if (n > k_max_args){
        return -1;
    } 
    
    size_t pos = 4;  
    while (n--){
        if (pos + 4 > reqlen){
            return -1;
        }

        uint32_t sz = 0;
        memcpy(&sz,&data[pos],4);
        pos += 4; // pos = 8 + 3 =  11
        if (sz + pos > reqlen){
            return -1;
        } 

        out.push_back(std::string((char*) &data[pos],sz));
        pos += sz;
    } 

    if (pos != reqlen) {
        return -1;  // trailing garbage
    }
    return 0; // out is['set','name','rishi']  
} 

static bool cmd_is(const std::string &word, const char *cmd){
    return 0 == strcasecmp(word.c_str(),cmd);
} 


static void do_get(std::vector<std::string> &cmd, std::string &out ){

    Entry key;

    key.key.swap(cmd[1]);   
    key.node.hcode = str_hash((uint8_t*)key.key.data(),key.key.size());

    HNode *node = hm_lookup(&g_data.db,&key.node,entry_eq);

    if (!node) {
        return out_nil(out);
    }
    Entry *ent = container_of(node, Entry, node);
    if (ent->type != T_STR) {
        return out_err(out, ERR_TYPE, "expect string type");
    }
    return out_kv(out,ent->key,ent->value);
} 

static void do_set(std::vector<std::string> &cmd, std::string &out ){

    Entry key;
    key.key.swap(cmd[1]);   
    key.node.hcode = str_hash((uint8_t*)key.key.data(),key.key.size());

    HNode *node = hm_lookup(&g_data.db,&key.node,entry_eq);

    if (node) {
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != T_STR) {
            return out_err(out, ERR_TYPE, "expect string type");
        }
        ent->value.swap(cmd[2]);
    } else{
        Entry *ent = new Entry();
        ent->key = key.key;
        ent->node.hcode = key.node.hcode;
        ent->value.swap(cmd[2]);
        hm_insert(&g_data.db,&ent->node);
    } 
    return out_nil(out);
} 

static bool str2dbl(const std::string &s, double &out) {
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}

static bool str2int(const std::string &s, int64_t &out) {
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}
// zadd zset score name
static void do_zadd(std::vector<std::string> &cmd,std::string &out){

    double score = 0;

    if (!str2dbl(cmd[2],score)){
        out_err(out,ERR_ARG,"expect fp number");
    } 
    // look up or create the zset
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode* hnode = hm_lookup(&g_data.db,&key.node,&entry_eq); 
    Entry *ent = NULL;

    if (!hnode){ // if not insert key in hastable
        ent = new Entry();
        ent->key.swap(key.key);
        ent->node.hcode =key.node.hcode;
        ent->type = T_ZSET; // setting to avl tree
        ent->zset = new ZSet(); // intiate a avl tree 
        hm_insert(&g_data.db,&ent->node);
    } else{
        ent = container_of(hnode, Entry, node);
        if (ent->type != T_ZSET) {
            return out_err(out, ERR_TYPE, "expect zset");
        }
    }
    
    const std::string &name = cmd[3];
    bool added = zset_add(ent->zset, name.data(), name.size(), score);
    return out_int(out, (int64_t)added);
}


static void entry_set_ttl(Entry* ent,int64_t ttl_ms){

    if (ttl_ms < 0 && ent->heap_idx != (size_t)-1){
        // erase an item from the heap
        size_t pos = ent->heap_idx;
        g_data.heap[pos] =  g_data.heap.back();
        g_data.heap.pop_back();
        if (pos < g_data.heap.size()) {
            heap_update(g_data.heap.data(), pos, g_data.heap.size());
        }
        ent->heap_idx = -1;
    } 
    else if (ttl_ms >= 0) {
        size_t pos = ent->heap_idx;
        if (pos == (size_t)-1) {
            HeapItem item;
            item.ref = &ent->heap_idx;
            g_data.heap.push_back(item);
            pos = g_data.heap.size() - 1;
        }
        // convert millisecond int microseconds by *1000
        g_data.heap[pos].val = get_monotonic_usec()+ (uint64_t)ttl_ms * 1000; // current time + time to live 30,000 millisecod - 30 sec
        heap_update(g_data.heap.data(), pos, g_data.heap.size());
    } 

} 

static void entry_del(Entry *ent) {
    switch (ent->type) {
    case T_ZSET:
        zset_dispose(ent->zset);
        delete ent->zset;
        break;
    }
    entry_set_ttl(ent, -1);
    delete ent;
}

static void do_del(std::vector<std::string> &cmd, std::string &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_pop(&g_data.db, &key.node, &entry_eq);
    if (node) {
        entry_del(container_of(node, Entry, node));
    }
    return out_int(out, node ? 1 : 0);
}


// pexpire name-1 1000ms-2
static void do_expire(std::vector<std::string> &cmd, std::string &out){
    int64_t ttl_ms = 0;
    if (!str2int(cmd[2], ttl_ms)) {
        return out_err(out, ERR_ARG, "expect int64");
    }

    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(),key.key.size());
    HNode* node = hm_lookup(&g_data.db,&key.node,&entry_eq);
    if (node){
        Entry *ent = container_of(node,Entry,node);
        entry_set_ttl(ent, ttl_ms);
    }
    return out_int(out, node ? 1: 0); 
}  

// get the time avialable before expiration
static void do_ttl(std::vector<std::string> &cmd, std::string &out){
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(),key.key.size());
    HNode* node = hm_lookup(&g_data.db,&key.node,&entry_eq);
    if (!node) {
        return out_int(out, -2); //If the key does not exist, send t -2.
    }

    Entry *ent = container_of(node, Entry, node); //If the key exist and ttl does not exist -1.
    if (ent->heap_idx == (size_t)-1) {
        return out_int(out, -1);
    }
    size_t pos = ent->heap_idx;
    uint64_t expire_at = g_data.heap[pos].val;
    uint64_t now_us = get_monotonic_usec();
    return  out_int(out, expire_at > now_us ? (expire_at - now_us) / 1000 : 0); // delta of time exist in  milliseconds.
} 


static bool expect_zset(std::string &out, std::string &s, Entry **ent) {
    Entry key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!hnode) {
        out_nil(out);
        return false;
    }

    *ent = container_of(hnode, Entry, node);
    if ((*ent)->type != T_ZSET) {
        out_err(out, ERR_TYPE, "expect zset");
        return false;
    }
    return true;
}

static void do_zrem(std::vector<std::string> &cmd, std::string &out) {
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        return;
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_pop(ent->zset, name.data(), name.size());
    if (znode) {
        znode_del(znode);
    }
    return out_int(out, znode ? 1 : 0);
}

static void do_zscore(std::vector<std::string> &cmd, std::string &out) {
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        return;
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(ent->zset, name.data(), name.size());
    return znode ? out_dbl(out, znode->score) : out_nil(out); // send double value in score
}


// zquery zset score name offset limit
static void do_zquery(std::vector<std::string> &cmd, std::string &out) {
    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_ARG, "expect fp number");
    }
    const std::string &name = cmd[3];
    int64_t offset = 0;
    int64_t limit = 0;
    if (!str2int(cmd[4], offset)) {
        return out_err(out, ERR_ARG, "expect int");
    }
    if (!str2int(cmd[5], limit)) {
        return out_err(out, ERR_ARG, "expect int");
    }

    // get the zset
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        if (out[0] == SER_NIL) {
            out.clear();
            out_arr(out, 0);
        }
        return;
    }

    if (limit <= 0) {
        return out_arr(out, 0);
    }
    ZNode *znode = zset_query(ent->zset, score, name.data(), name.size());
    znode = znode_offset(znode, offset);

    // output
    void *arr = begin_arr(out);
    uint32_t n = 0;
    while (znode && (int64_t)n < limit) {
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = znode_offset(znode, +1);
        n += 2;
    }
    end_arr(out, arr, n);
}

// find all the key's in the hashtables - linked lists
static void h_scan(HTab *tab, void (*f)(HNode *, void *), void *arg) {
    if (tab->size == 0) {
        return;
    }
    for (size_t i = 0; i < tab->mask + 1; ++i) {
        HNode *node = tab->tab[i];
        while (node) {
            f(node, arg);
            node = node->next;
        }
    }
}

static void cb_scan(HNode *node, void *arg) {
    std::string &out = *(std::string *)arg;
    //out_str(out, container_of(node, Entry, node)->key);
    Entry* ent = container_of(node, Entry, node);
    out_kv(out,ent->key,ent->value);
}

static void do_keys(std::vector<std::string> &cmd, std::string &out) {
    (void)cmd;
    out_arr(out, (uint32_t)hm_size(&g_data.db));
    h_scan(&g_data.db.ht1, &cb_scan, &out);
    h_scan(&g_data.db.ht2, &cb_scan, &out);
}

static void do_request(std::vector<std::string> &cmd, std::string &out) {
    if (cmd.size() == 1 && cmd_is(cmd[0], "keys")) {
        do_keys(cmd, out);
    } else if (cmd.size() == 2 && cmd_is(cmd[0], "get")) {
        do_get(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "set")) {
        do_set(cmd, out);
    } else if (cmd.size() == 2 && cmd_is(cmd[0], "del")) {
        do_del(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "pexpire")) {
        do_expire(cmd, out);
    } else if (cmd.size() == 2 && cmd_is(cmd[0], "pttl")) {
        do_ttl(cmd, out);
    } else if (cmd.size() == 4 && cmd_is(cmd[0], "zadd")) {
        do_zadd(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "zrem")) {
        do_zrem(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "zscore")) {
        do_zscore(cmd, out);
    } else if (cmd.size() == 6 && cmd_is(cmd[0], "zquery")) {
        do_zquery(cmd, out);
    } else {
        // cmd is not recognized
        out_err(out, ERR_UNKNOWN, "Unknown cmd");
    }
}

static bool try_one_request(Conn *conn) {
   
    if (conn->rbuf_size < 4) {
        // not enough data in the buffer
        return false;
    }
    uint32_t len = 0;
    memcpy(&len, &conn->rbuf[0], 4);
    if (len > k_max_msg) {
        msg("too long");
        conn->state = STATE_END;
        return false;
    }

    if (4 + len > conn->rbuf_size) { 
        // checking if the data exist in the buffer 
        // not enough data in buffer . will retry in the next iteration
        return false;
    }

    std::vector<std::string> cmd;  
    
    if (0 != parse_req(&conn->rbuf[4],len,cmd)){
        msg("bad req");
        conn->state = STATE_END;
        return false; // fixed here!
    } 
    std::string out;
    do_request(cmd, out);

    // pack the response into the buffer
    if (4 + out.size() > k_max_msg) {
        out.clear();
        out_err(out, ERR_2BIG, "response is too big");
    }
    
    uint32_t wlen = (uint32_t)out.size();
    memcpy(&conn->wbuf[0], &wlen, 4);
    memcpy(&conn->wbuf[4], out.data(), out.size());
    conn->wbuf_size = 4 + wlen;

  
    size_t remain = conn->rbuf_size - 4 - len;
    if (remain) {
        memmove(conn->rbuf, &conn->rbuf[4 + len], remain);
    }
    conn->rbuf_size = remain;

    // change state
    conn->state = STATE_RES;
    state_res(conn);
    return (conn->state == STATE_REQ);
}

// fill the the entire read buffer
static bool try_fill_buffer(Conn *conn) {
    
    assert(conn->rbuf_size < sizeof(conn->rbuf));
    ssize_t rv = 0;
    do {
        size_t cap = sizeof(conn->rbuf) - conn->rbuf_size; // intially 4100 - 0 = 4100
        rv = read(conn->fd, &conn->rbuf[conn->rbuf_size], cap);
    } while (rv < 0 && errno == EINTR);
    if (rv < 0 && errno == EAGAIN) {
        return false;
    }
    if (rv < 0) {
        msg("read() error");
        conn->state = STATE_END;
        return false;
    }
    if (rv == 0) {
        if (conn->rbuf_size > 0) {
            msg("unexpected EOF");
        } else {
            msg("EOF");
        }
        conn->state = STATE_END;
        return false;
    }

    conn->rbuf_size += (size_t)rv;
    assert(conn->rbuf_size <= sizeof(conn->rbuf)); // the message read should be less than rbuf size

    while (try_one_request(conn)) {}
    return (conn->state == STATE_REQ);
}

static void state_req(Conn *conn) {
    while (try_fill_buffer(conn)) {}
}


static bool try_flush_buffer(Conn *conn) {
    while (conn->wbuf_sent < conn->wbuf_size) {
        ssize_t rv = write(conn->fd, &conn->wbuf[conn->wbuf_sent], conn->wbuf_size - conn->wbuf_sent);
        if (rv == -1) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN) return true;
            msg("write() error");
            conn->state = STATE_END;
            return false;
        }
        conn->wbuf_sent += (size_t)rv;
    }

    conn->state = STATE_REQ;
    conn->wbuf_sent = 0;
    conn->wbuf_size = 0;
    return false;
}


static void state_res(Conn *conn) {
    while (try_flush_buffer(conn)) {}
}

static void connection_io(Conn *conn) {
    conn->idle_start = get_monotonic_usec();
    dlist_detach(&conn->idle_list);
    dlist_insert_before(&g_data.idle_list, &conn->idle_list);

    if (conn->state == STATE_REQ) {
        state_req(conn);
    } else if (conn->state == STATE_RES) {
        state_res(conn);
    } else {
        assert(0);
    }
}

const uint64_t k_idle_timeout_ms = 60 * 1000;

static uint32_t next_timer_ms() {
    if (dlist_empty(&g_data.idle_list)) {
        //printf("No timers. Default timeout: 10000ms\n");
        return k_idle_timeout_ms;   // no timer, the value doesn't matter
    }

    uint64_t now_us = get_monotonic_usec();
    Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
    uint64_t next_us = next->idle_start + k_idle_timeout_ms * 1000;
    if (next_us <= now_us) {

        return 0;
    }

    uint32_t timeout = (uint32_t)((next_us - now_us) / 1000);
    return timeout;
}


static void conn_done(Conn *conn) {
 
    g_data.fd2conn[conn->fd] = NULL;
    (void)close(conn->fd);
    dlist_detach(&conn->idle_list);
    free(conn);
    
}

static bool hnode_same(HNode *lhs, HNode *rhs) {
    return lhs == rhs;
}
static void process_timers() {
    uint64_t now_us = get_monotonic_usec();
    while (!dlist_empty(&g_data.idle_list)) {
        Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
        uint64_t next_us = next->idle_start + k_idle_timeout_ms * 1000;
        if (next_us >= now_us + 1000) {
            // not ready, the extra 1000us is for the ms resolution of poll()
            break;
        }
        conn_done(next);
    }

    const size_t k_max_works = 2000;
    size_t nworks = 0;
    while (!g_data.heap.empty() && g_data.heap[0].val < now_us) {
        Entry *ent = container_of(g_data.heap[0].ref, Entry, heap_idx);
        std::cout<< ent->key << std::endl;
        HNode *node = hm_pop(&g_data.db, &ent->node, &entry_eq);

        assert(node == &ent->node);
        entry_del(ent);
        if (nworks++ >= k_max_works) {
            // don't stall the server if too many keys are expiring at once
            break;
        }
    }
}


int main(int argc, char *argv[]) {


  
    if (argc > 1 && strcmp(argv[1], "help") == 0) {
        printf("Usage: ./kvserver [help]\n\n");
        printf("Available commands:\n");
        printf("  set <key> <value>       - Set a string value\n");
        printf("  get <key>               - Get a string value\n");
        printf("  del <key>               - Delete a key\n");
        printf("  pexpire <key> <ms>      - Set a key to expire in ms\n");
        printf("  pttl <key>              - Get TTL of a key\n");
        printf("  zadd <zset> <score> <member> - Add member to sorted set\n");
        printf("  zrem <zset> <member>    - Remove member from sorted set\n");
        printf("  zscore <zset> <member>  - Get score of member\n");
        printf("  zquery <zset> <score> <member> <offset> <limit> - Range query\n");
        printf("  keys                    - List all keys\n");
        printf("\nStart the server by simply running: ./kvserver\n");
        return 0;
    }

    dlist_init(&g_data.idle_list);
    
    int fd = socket(AF_INET, SOCK_STREAM, 0); // create a server socket 
    if (fd < 0) {
        die("socket()");
    }

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // bind
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = ntohl(0);    // wildcard address 0.0.0.0
    int rv = bind(fd, (const sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("bind()");
    }

    // listen
    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }

    fd_set_nb(fd); // set the server to non blocking 

    // Create epoll instance
    int epfd = epoll_create1(0);
    if (epfd < 0) {
        die("epoll_create1()");
    }

    // Register the listening socket
    struct epoll_event event = {};
    event.data.fd = fd;
    event.events = EPOLLIN;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) < 0) {
        close(fd); // Clean up server socket
        close(epfd); // Clean up epoll instance
        die("epoll_ctl() error");
    }

    // the event loop
    struct epoll_event events[MAX_EVENTS];
    printf("%s\n","the server is listening");
    signal(SIGINT, handle_signal);

    while (!stop) {
        int timeout_ms = (int)next_timer_ms(); // Ingnoring timeouts
      
        int nfds = epoll_wait(epfd, events, MAX_EVENTS,-1); // blocking operation 
     
        if (nfds < 0) {
            if (errno == EINTR) continue; // Interrupted by signal, retry
            die("epoll_wait()");
        }

        for (int i = 0; i < nfds; ++i) {
            if (events[i].data.fd == fd) {    // if fd is server fd
                // The listening fd is ready, try to accept new connections
                accept_new_conn(g_data.fd2conn, epfd, fd);
            } else {
                // A client connection is ready
                Conn *conn = g_data.fd2conn[events[i].data.fd];
                connection_io(conn);
                if (conn->state == STATE_END) {
                    // Client closed the connection or an error occurred
                    conn_done(conn);
                }
            }
        }
        
        // handle timers
        // process_timers();
    }
    close(epfd);
    close(fd);
   
    return 0;
}
