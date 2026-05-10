# ProcX - Advanced Linux Process Manager

ProcX is a command-line process manager written in C for Linux systems. It was developed as a semester project for the **Operating Systems** course in the 3rd year of Computer Engineering.

The project demonstrates core operating system concepts such as process creation, inter-process communication (IPC), synchronization, multithreading, and signal handling.

---

##  Features

- Start new programs using `fork()` and `execvp()`
-  Run programs in:
- Attached mode
- Detached mode (background session using `setsid()`)
-  List all active processes
-  Terminate processes by PID
-  Monitor child processes automatically
-  Remove terminated (ghost) processes from shared memory
-  Send notifications between ProcX instances using message queues
-  Synchronize shared memory access with POSIX semaphores
-  Use multiple threads for monitoring and IPC communication
-  Safe and validated user input

---

##  Technologies Used

- C Programming Language
- POSIX Threads (`pthread`)
- System V Shared Memory (`shmget`, `shmat`)
- System V Message Queues (`msgget`, `msgsnd`, `msgrcv`)
- POSIX Named Semaphores (`sem_open`)
- Unix Signals (`SIGTERM`, `SIGINT`)
- Linux Process Management APIs

---

##  System Architecture

ProcX uses several operating system mechanisms together:

### Shared Memory
Stores information about all running processes.

### Message Queue
Allows multiple ProcX instances to send notifications to each other.

### Semaphore
Protects shared memory from race conditions.

### Monitor Thread
Checks terminated child processes using `waitpid()`.

### IPC Thread
Receives and prints notifications from other ProcX instances.

---

##  Process Information Stored

Each process is represented by the following data:

- PID
- Owner PID (the ProcX instance that started the process)
- Command
- Execution Mode (Attached / Detached)
- Status (Running / Terminated)
- Start Time
- Active Flag

