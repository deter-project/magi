#include "logger.h"

Logger * Logger_create( FILE* fp, int log_level )
{
	Logger *l = (Logger *)malloc(sizeof(Logger));
	if ( l == NULL )
	return NULL;
	l->datetime_format = (char *)"%Y-%m-%d %H:%M:%S";
	if(log_level >= 0 && log_level < 4)
		l->level = log_level;
	else
		l->level = LOG_INFO;/*set default log level*/
	if(fp != NULL)
		l->fp = fp;
	else
		l->fp = stdout;
	return l;
}

void Logger_free(Logger *l)
{
	if ( l != NULL )
	{
		if ( fileno(l->fp) != STDOUT_FILENO )
			fclose(l->fp);
		free(l);
	}
}

void log_add(Logger *l, int level, const char *msg)
{
	if (level < l->level) 
		return;
	time_t meow = time(NULL);
	char buf[64];
	strftime(buf, sizeof(buf), l->datetime_format, localtime(&meow));
	fprintf(l->fp, "%s : %s\n",buf,msg);

	fflush(l->fp);
}

void log_debug(Logger *l, const char *fmt, ...)
{
	va_list ap;
	char msg[LOG_MAX_MSG_LEN];
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	log_add(l, LOG_DEBUG, msg);
	va_end(ap);
}

void log_info(Logger *l, const char *fmt, ...)
{
	va_list ap;
	char msg[LOG_MAX_MSG_LEN];
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	log_add(l, LOG_INFO, msg);
	va_end(ap);
}

void log_warn(Logger *l, const char *fmt, ...)
{
	va_list ap;
	char msg[LOG_MAX_MSG_LEN];
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	log_add(l, LOG_WARN, msg);
	va_end(ap);
}

void log_error(Logger *l, const char *fmt, ...)
{
	va_list ap;
	char msg[LOG_MAX_MSG_LEN];
	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	log_add(l, LOG_ERROR, msg);
	va_end(ap);
}

/*int main()
{

	FILE* fp ;
	fp = fopen("test.log","w");
	Logger *n = Logger_create(fp,LOG_ERROR);
	//log_add(n,LOG_INFO,"info messages are logged\n");
	log_info(n,"Started logging...\n");
	log_warn(n,"WARNING: Entering untested code..\n");
	log_error(n,"Non- magi message received");
	log_debug(n,"debug msg");	
}*/
