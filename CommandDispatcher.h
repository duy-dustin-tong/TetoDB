// CommandDispatcher.h

#pragma once

#include "Common.h"
#include "Schema.h"

extern Database DB;

Result ProcessCommand(string &line);
void ProcessDotCommand(string &line);