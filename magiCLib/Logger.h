#ifndef _LOGGER_H_

#define _LOGGER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <stdarg.h>
#include <unistd.h>
#include <pthread.h>

#define LOG_DEBUG 0
#define LOG_INFO 1
#define LOG_WARN 2
#define LOG_ERROR 3
#define LOG_MAX_MSG_LEN 1024

struct _Logger {
	int level;
	pthread_mutex_t log_mutex;
	char *datetime_format;
	FILE *fp;
};
typedef struct _Logger Logger;

Logger * Logger_create(FILE* fp, int level);
void Logger_free(Logger *l);

void log_log(Logger *l, int level, const char *fmt, ...);
void log_debug(Logger *l, const char *fmt, ...);
void log_info(Logger *l, const char *fmt, ...);
void log_warn(Logger *l, const char *fmt, ...);
void log_error(Logger *l, const char *fmt, ...);

Logger* createLogger(char* path, char* fileName, FILE* fp, int log_level);
void destroyLogger(FILE* fp, Logger *logger);

int getIntLevel(char* strLevel);

#ifdef __cplusplus
}
#endif

#endif
