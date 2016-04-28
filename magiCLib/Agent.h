#ifndef _AGENT_H
#define _AGENT_H

#include "MAGIMessage.h"
#include "Util.h"

void registerFunction(char* name, char* retType, void* fptr, int argCount, ...);
void agentStart(int argc, char** argv);
dList_t* ArgParser(int argc, char** argv);

char* dispatchCall(const char *name, char** args, char* retType);
void doMessageAction(MAGIMessage_t* magiMsg);

void sendAgentLoadDoneTrigger();
void sendAgentUnloadDoneTrigger();

#endif /* _AGENT_H */

