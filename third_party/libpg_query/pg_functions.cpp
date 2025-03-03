#include <stdexcept>
#include <string>
#include <thread>
#include <mutex>
#include "pg_functions.hpp"
#include "parser/parser.hpp"
#include <stdarg.h>
#include <mutex>
#include <cstring>


// max parse tree size approx 100 MB, should be enough
#define PG_MALLOC_SIZE 10240

namespace duckdb_libpgquery {

typedef struct pg_parser_state_str parser_state;
struct pg_parser_state_str {
	int pg_err_code;
	int pg_err_pos;
	char pg_err_msg[BUFSIZ];

	size_t malloc_pos;
	size_t malloc_ptr_idx;
	char **malloc_ptrs;
	size_t malloc_ptr_size;
};

static __thread parser_state pg_parser_state;

#ifndef __GNUC__
__thread PGNode *duckdb_newNodeMacroHolder;
#endif

static void allocate_new(parser_state *state, size_t n) {
	if (state->malloc_ptr_idx >= state->malloc_ptr_size) {
		size_t new_size = state->malloc_ptr_size * 2;
		auto new_malloc_ptrs = (char **) malloc(sizeof(char *) * new_size);
		memcpy(new_malloc_ptrs, state->malloc_ptrs, state->malloc_ptr_size * sizeof(char*));
		free(state->malloc_ptrs);
		state->malloc_ptr_size = new_size;
		state->malloc_ptrs = new_malloc_ptrs;
	}
	if (n < PG_MALLOC_SIZE) {
		n = PG_MALLOC_SIZE;
	}
	char *base_ptr = (char *)malloc(n);
	if (!base_ptr) {
		throw std::runtime_error("Memory allocation failure");
	}
	state->malloc_ptrs[state->malloc_ptr_idx] = base_ptr;
	state->malloc_ptr_idx++;
	state->malloc_pos = 0;
}

void *palloc(size_t n) {
	// we need to align our pointers for the sanitizer
	auto aligned_n = ((n + 7) / 8) * 8;
	if (pg_parser_state.malloc_pos + aligned_n > PG_MALLOC_SIZE) {
		allocate_new(&pg_parser_state, aligned_n);
	}

	void *ptr = pg_parser_state.malloc_ptrs[pg_parser_state.malloc_ptr_idx - 1] + pg_parser_state.malloc_pos;
	memset(ptr, 0, n);
	pg_parser_state.malloc_pos += aligned_n;
	return ptr;
}

void pg_parser_init() {
	pg_parser_state.pg_err_code = PGUNDEFINED;
	pg_parser_state.pg_err_msg[0] = '\0';

	pg_parser_state.malloc_ptr_size = 4;
	pg_parser_state.malloc_ptrs = (char **) malloc(sizeof(char *) * pg_parser_state.malloc_ptr_size);
	pg_parser_state.malloc_ptr_idx = 0;
	allocate_new(&pg_parser_state, 1);
}

void pg_parser_parse(const char *query, parse_result *res) {
	res->parse_tree = nullptr;
	try {
		res->parse_tree = duckdb_libpgquery::raw_parser(query);
		res->success = pg_parser_state.pg_err_code == PGUNDEFINED;
	} catch (std::exception &ex) {
		res->success = false;
		// copy the error message of the exception
		auto error_message = ex.what();
		uint32_t pos = 0;
		while(pos < 1023 && error_message[pos]) {
			pg_parser_state.pg_err_msg[pos] = error_message[pos];
			pos++;
		}
		pg_parser_state.pg_err_msg[pos] = '\0';
	}
	res->error_message = pg_parser_state.pg_err_msg;
	res->error_location = pg_parser_state.pg_err_pos;
}

void pg_parser_cleanup() {
	for (size_t ptr_idx = 0; ptr_idx < pg_parser_state.malloc_ptr_idx; ptr_idx++) {
		char *ptr = pg_parser_state.malloc_ptrs[ptr_idx];
		if (ptr) {
			free(ptr);
			pg_parser_state.malloc_ptrs[ptr_idx] = nullptr;
		}
	}
	free(pg_parser_state.malloc_ptrs);
}

int ereport(int code, ...) {
	std::string err = "parser error : " + std::string(pg_parser_state.pg_err_msg);
	throw std::runtime_error(err);
}
void elog(int code, const char *fmt, ...) {
	throw std::runtime_error("elog NOT IMPLEMENTED");
}
int errcode(int sqlerrcode) {
	pg_parser_state.pg_err_code = sqlerrcode;
	return 1;
}
int errmsg(const char *fmt, ...) {
	va_list argptr;
	va_start(argptr, fmt);
	vsnprintf(pg_parser_state.pg_err_msg, BUFSIZ, fmt, argptr);
	va_end(argptr);
	return 1;
}
int errhint(const char *msg) {
	throw std::runtime_error("errhint NOT IMPLEMENTED");
}
int errmsg_internal(const char *fmt, ...) {
	throw std::runtime_error("errmsg_internal NOT IMPLEMENTED");
}
int errdetail(const char *fmt, ...) {
	throw std::runtime_error("errdetail NOT IMPLEMENTED");
}
int errposition(int cursorpos) {
	pg_parser_state.pg_err_pos = cursorpos;
	return 1;
}

char *psprintf(const char *fmt, ...) {
	char buf[BUFSIZ];
	va_list args;
	size_t newlen;

	// attempt one: use stack buffer and determine length
	va_start(args, fmt);
	newlen = vsnprintf(buf, BUFSIZ, fmt, args);
	va_end(args);
	if (newlen < BUFSIZ) {
		return pstrdup(buf);
	}

	// attempt two, malloc
	char *mbuf = (char *)palloc(newlen);
	va_start(args, fmt);
	vsnprintf(mbuf, newlen, fmt, args);
	va_end(args);
	return mbuf;
}

char *pstrdup(const char *in) {
	char *new_str = (char *)palloc(strlen(in) + 1);
	memcpy(new_str, in, strlen(in));
	return new_str;
}

void pfree(void *ptr) {
	// nop, we free up entire context on parser cleanup
}
void *palloc0fast(size_t n) { // very fast
	return palloc(n);
}
void *repalloc(void *ptr, size_t n) {
	return palloc(n);
}
char *NameListToString(PGList *names) {
	throw std::runtime_error("NameListToString NOT IMPLEMENTED");
}
void *copyObject(const void *from) {
	throw std::runtime_error("copyObject NOT IMPLEMENTED");
}
bool equal(const void *a, const void *b) {
	throw std::runtime_error("equal NOT IMPLEMENTED");
}
int exprLocation(const PGNode *expr) {
	throw std::runtime_error("exprLocation NOT IMPLEMENTED");
}
bool pg_verifymbstr(const char *mbstr, int len, bool noError) {
	throw std::runtime_error("pg_verifymbstr NOT IMPLEMENTED");
}

int pg_database_encoding_max_length(void) {
	return 4; // UTF8
}

static int pg_utf_mblen(const unsigned char *s) {
	int len;

	if ((*s & 0x80) == 0)
		len = 1;
	else if ((*s & 0xe0) == 0xc0)
		len = 2;
	else if ((*s & 0xf0) == 0xe0)
		len = 3;
	else if ((*s & 0xf8) == 0xf0)
		len = 4;
#ifdef NOT_USED
	else if ((*s & 0xfc) == 0xf8)
		len = 5;
	else if ((*s & 0xfe) == 0xfc)
		len = 6;
#endif
	else
		len = 1;
	return len;
}

int pg_mbstrlen_with_len(const char *mbstr, int limit) {
	int len = 0;
	while (limit > 0 && *mbstr) {
		int l = pg_utf_mblen((const unsigned char *)mbstr);
		limit -= l;
		mbstr += l;
		len++;
	}
	return len;
}

int pg_mbcliplen(const char *mbstr, int len, int limit) {
	throw std::runtime_error("pg_mbcliplen NOT IMPLEMENTED");
}
int pg_mblen(const char *mbstr) {
	throw std::runtime_error("pg_mblen NOT IMPLEMENTED");
}
PGDefElem *defWithOids(bool value) {
	throw std::runtime_error("defWithOids NOT IMPLEMENTED");
}
unsigned char *unicode_to_utf8(pg_wchar c, unsigned char *utf8string) {
	throw std::runtime_error("unicode_to_utf8 NOT IMPLEMENTED");
}

// this replaces a brain damaged macro in nodes.hpp
PGNode *newNode(size_t size, PGNodeTag type) {
	auto result = (PGNode *)palloc0fast(size);
	result->type = type;
	return result;
}
}