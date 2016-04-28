#ifndef _AGENT_MESSENGER_H
#define _AGENT_MESSENGER_H

#include "MAGIMessage.h"

MAGIMessage_t* next(int block);

void sendMsg(MAGIMessage_t* magiMsg, char* arg, ...);
void trigger(char* groups, char* docks, contentType_t contenttype, char* data);

void joinGroup(char* group);
void leaveGroup(char* group);
void listenDock(char* dock);
void unlistenDock(char* dock);

#endif /* _AGENT_MESSENGER_H */
