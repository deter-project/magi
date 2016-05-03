#include "Logger.h"
#include "Util.h"

#include <string.h>

Logger* Logger_create(FILE* fp, int log_level) {
	Logger *l = (Logger *) malloc(sizeof(Logger));
	if (l == NULL)
		return NULL;
	pthread_mutex_init(&l->log_mutex, NULL);
	l->datetime_format = (char *) "%Y-%m-%d %H:%M:%S";
	if (log_level >= 0 && log_level < 4)
		l->level = log_level;
	else
		l->level = LOG_INFO;/*set default log level*/
	if (fp != NULL)
		l->fp = fp;
	else
		l->fp = stdout;
	return l;
}

void Logger_free(Logger *l) {
	if (l != NULL) {
		if (fileno(l->fp) != STDOUT_FILENO)
			fclose(l->fp);
		free(l);
	}
}

void log_add(Logger *l, int level, const char *msg) {
	if (level < l->level)
		return;

	time_t meow = time(NULL);
	char timeStr[64];
	strftime(timeStr, sizeof(timeStr), l->datetime_format, localtime(&meow));

	pthread_mutex_lock(&l->log_mutex);

	char* levelStr = (char*) malloc(6);
	if (level == 0) {
		strcpy(levelStr, "DEBUG");
	} else if (level == 1) {
		strcpy(levelStr, "INFO ");
	} else if (level == 2) {
		strcpy(levelStr, "WARN ");
	} else if (level == 3) {
		strcpy(levelStr, "ERROR");
	} else {
		strcpy(levelStr, "UNKWN");
	}

	fprintf(l->fp, "%s : %s : %s\n", timeStr, levelStr, msg);
	fflush(l->fp);

	free(levelStr);

	pthread_mutex_unlock(&l->log_mutex);
}

void log_log(Logger *l, int level, const char *fmt, ...) {
	va_list ap;
	char msg[LOG_MAX_MSG_LEN];
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	log_add(l, level, msg);
	va_end(ap);
}

void log_debug(Logger *l, const char *fmt, ...) {
	va_list ap;
	char msg[LOG_MAX_MSG_LEN];
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	log_add(l, LOG_DEBUG, msg);
	va_end(ap);
}

void log_info(Logger *l, const char *fmt, ...) {
	va_list ap;
	char msg[LOG_MAX_MSG_LEN];
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	log_add(l, LOG_INFO, msg);
	va_end(ap);
}

void log_warn(Logger *l, const char *fmt, ...) {
	va_list ap;
	char msg[LOG_MAX_MSG_LEN];
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	log_add(l, LOG_WARN, msg);
	va_end(ap);
}

void log_error(Logger *l, const char *fmt, ...) {
	va_list ap;
	char msg[LOG_MAX_MSG_LEN];
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	log_add(l, LOG_ERROR, msg);
	va_end(ap);
}

Logger* createLogger(char* path, char* fileName, FILE* fp, int log_level) {
	Logger* logger;
	char logFileStr[100];
	sprintf(logFileStr, "%s/%s", path, fileName);
	fp = fopen(logFileStr, "a");
	logger = Logger_create(fp, log_level);
	return logger;
}

void destroyLogger(FILE* fp, Logger *logger) {
	fclose(fp);
	Logger_free(logger);
}

int getIntLevel(char* strLevel){
	if(strLevel == NULL){
		return LOG_INFO;
	}
	if (strcmp("DEBUG", covertToUpper(strLevel)) == 0){
		return LOG_DEBUG;
	} else if (strcmp("WARN", covertToUpper(strLevel)) == 0){
		return LOG_WARN;
	} else if (strcmp("ERROR", covertToUpper(strLevel)) == 0){
		return LOG_ERROR;
	} else {
		return LOG_INFO;
	}
}
