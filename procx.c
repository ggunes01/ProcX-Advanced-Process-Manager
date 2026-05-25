
#define _XOPEN_SOURCE 700 //posix standarts
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <time.h>

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <sys/wait.h>

#include <fcntl.h>
#include <semaphore.h>

/* According to project guideline constans and structs */

#define MAX_PROCS 50
#define CMD_LEN 256 //max 256 char for order

#define SHM_KEY_FILE "procx_shm_keyfile"
#define MQ_KEY_FILE  "procx_mq_keyfile"

#define SHM_PROJ_ID  0xA1 //Shared memory and its unique key
#define MQ_PROJ_ID   0xB2 //Message queue 

#define SEM_NAME     "/procx_sem_final_v1" //semaphore

typedef enum { MODE_ATTACHED = 0, MODE_DETACHED = 1 } ProcessMode;
typedef enum { STATUS_RUNNING = 0, STATUS_TERMINATED = 1 } ProcessStatus;

typedef struct {
    pid_t pid;
    pid_t owner_pid;
    char command[CMD_LEN];
    ProcessMode mode;
    ProcessStatus status;
    time_t start_time;
    int is_active;
} ProcessInfo;

typedef struct {
    ProcessInfo processes[MAX_PROCS];
    int process_count;
} SharedData;

typedef struct {
    long mtype;
    pid_t sender;
    int command;
    pid_t target_pid;
    char text[128];
} MQMsg;

/* GLOBAL VARIABLES */

static int shmid = -1;
static SharedData *shm = NULL; // the pointer that mapped shared memory

static int mqid = -1; // Currently that is not occured( shmget())

static sem_t *gsem = NULL;

static int created_shm = 0;
static int created_mq = 0;
static int created_sem = 0;

static pthread_t monitor_thr, ipc_thr;

static volatile sig_atomic_t running = 1;
static pid_t self_pid;


/* SAFE INPUT HELPERS (Those methods prevent error that occured by I/O)*/

static int safe_getline(char *buf, size_t size) { /* That method created to clean empty spaces from input*/
    if (!fgets(buf, size, stdin)) return 0;
    buf[strcspn(buf, "\n")] = 0;
    return 1;
}

static int parse_int_strict(const char *s, long *out) { /* That method created to accept just integer values in input*/
    while (isspace((unsigned char)*s)) s++;
    if (*s == '\0') return 0;

    char *endptr;
    errno = 0;
    long val = strtol(s, &endptr, 10);
    if (errno != 0) return 0;

    while (isspace((unsigned char)*endptr)) endptr++;
    if (*endptr != '\0') return 0;

    *out = val;
    return 1;
}

static int ask_menu_choice(int min, int max) { /* That method created to get a integer between 2 different numbers*/
    char line[64];
    long v;
    while (running) {
        printf("Your choice: ");
        if (!safe_getline(line, sizeof(line))) continue;

        if (!parse_int_strict(line, &v) || v < min || v > max) {
            printf("[ERROR] Enter a number between %d and %d.\n", min, max);
            continue;
        }
        return (int)v;
    }
    return min;
}

static int ask_mode() { /* That method created to get working mode from user */
    char line[64];
    long v;
    while (running) {
        printf("Choose running mode (0: Attached, 1: Detached): ");
        if (!safe_getline(line, sizeof(line))) continue;

        if (!parse_int_strict(line, &v) || (v != 0 && v != 1)) {
            printf("[ERROR] Mode must be 0 or 1.\n");
            continue;
        }
        return (int)v;
    }
    return 0;
}

static pid_t ask_pid() { /* That method created to get a valid pid from user*/
    char line[64];
    long v;
    while (running) {
        printf("Enter PID: ");
        if (!safe_getline(line, sizeof(line))) continue;

        if (!parse_int_strict(line, &v) || v <= 0) {
            printf("[ERROR] Enter a valid PID.\n");
            continue;
        }
        return (pid_t)v;
    }
    return -1;
}


/* IPC SETUP (That method creted to provide a good struct for communucation between processes) */

static void setup_ipc() {
    FILE *f;

    /* Ensure key files exist */
    f = fopen(SHM_KEY_FILE, "a"); if (f) fclose(f);
    f = fopen(MQ_KEY_FILE, "a"); if (f) fclose(f);

    key_t shmkey = ftok(SHM_KEY_FILE, SHM_PROJ_ID);
    key_t mqkey  = ftok(MQ_KEY_FILE,  MQ_PROJ_ID);

    /* Shared memory */
    shmid = shmget(shmkey, sizeof(SharedData), 0666 | IPC_CREAT | IPC_EXCL);
    if (shmid >= 0) created_shm = 1;
    else {
        if (errno == EEXIST)
            shmid = shmget(shmkey, sizeof(SharedData), 0666 | IPC_CREAT);
        else { perror("shmget"); exit(1); }
    }

    shm = (SharedData*) shmat(shmid, NULL, 0);
    if (shm == (void*)-1) { perror("shmat"); exit(1); }

    if (created_shm) {
        shm->process_count = 0;
        for (int i = 0; i < MAX_PROCS; ++i)
            shm->processes[i].is_active = 0;
    }

    /* Message queue */
    mqid = msgget(mqkey, 0666 | IPC_CREAT | IPC_EXCL);
    if (mqid >= 0) created_mq = 1;
    else {
        if (errno == EEXIST)
            mqid = msgget(mqkey, 0666 | IPC_CREAT);
        else { perror("msgget"); exit(1); }
    }

    /* Semaphore */
    gsem = sem_open(SEM_NAME, O_CREAT | O_EXCL, 0666, 1);
    if (gsem == SEM_FAILED) {
        if (errno == EEXIST)
            gsem = sem_open(SEM_NAME, 0);
        else { perror("sem_open"); exit(1); }
    } else created_sem = 1;
}


/* SHARED MEMORY OPERATIONS */

static void compact_list() {  /* aktif processleri sıkıştır ve listede düzenli hale getir is_active == 0 olan boşlukları kaldır çünkü remove_pid silmez*/
    int w = 0;
    for (int r = 0; r < shm->process_count; ++r) {
        if (shm->processes[r].is_active) {
            if (w != r)
                shm->processes[w] = shm->processes[r];
            w++;
        }
    }
    for (int i = w; i < shm->process_count; ++i)
        shm->processes[i].is_active = 0;
    shm->process_count = w;
}

static int add_proc(const ProcessInfo *p) { /* that method adds new process to s m process list*/
    int ok = -1;
    sem_wait(gsem);

    if (shm->process_count < MAX_PROCS) {
        shm->processes[shm->process_count] = *p;
        shm->processes[shm->process_count].is_active = 1;
        shm->process_count++;
        ok = 0;
    }

    sem_post(gsem);
    return ok;
}

static void remove_pid(pid_t pid) { /*that method removes terminated process from shared memory*/
    sem_wait(gsem);
    for (int i = 0; i < shm->process_count; ++i) {
        if (shm->processes[i].is_active && shm->processes[i].pid == pid) {
            shm->processes[i].is_active = 0;
            shm->processes[i].status = STATUS_TERMINATED;
        }
    }
    compact_list();
    sem_post(gsem);
}


/* IPC SEND*/ /* that method sends messages by message queue*/

static void send_mq(int cmd, pid_t target, const char *text) {
    MQMsg m = {0};
    m.mtype = 1;
    m.sender = self_pid;
    m.command = cmd;
    m.target_pid = target;
    if (text) strncpy(m.text, text, sizeof(m.text)-1);

    msgsnd(mqid, &m, sizeof(MQMsg)-sizeof(long), 0);
}


/*  THREADS */

static void *monitor_thread(void *a) {
    (void)a;
    while (running) {
        int status;
        pid_t pid;
        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
            remove_pid(pid);
            printf("[MONITOR] PID %d terminated\n", pid);
            send_mq(2, pid, "terminated");
        }
        sleep(2);
    }
    return NULL;
}

static void *ipc_thread(void *a) {
    (void)a;
    while (running) {
        MQMsg m;
        ssize_t r = msgrcv(mqid, &m, sizeof(MQMsg)-sizeof(long), 0, IPC_NOWAIT);
        if (r == -1) {
            if (errno == ENOMSG) { usleep(150000); continue; }
            break;
        }
        if (m.sender != self_pid) {
            if (m.command == 1)
                printf("[IPC] Started PID %d by %d\n", m.target_pid, m.sender);
            else if (m.command == 2)
                printf("[IPC]  Terminated PID %d\n", m.target_pid);
        }
    }
    return NULL;
}


/* GHOST PID CLEANUP */ /* clean ghost pids from sm*/

static void cleanup_dead_pids() {
    sem_wait(gsem);
    for (int i = 0; i < shm->process_count; ++i) {
        ProcessInfo *p = &shm->processes[i];

        if (p->is_active) {
            if (kill(p->pid, 0) == -1 && errno == ESRCH) {
                p->is_active = 0;
                p->status = STATUS_TERMINATED;
            }
        }
    }
    compact_list();
    sem_post(gsem);
}


/* PROCESS FUNCTIONS */

static void start_process() {
    char cmd[CMD_LEN];

    while (1) {
        printf("Enter command: ");
        if (!safe_getline(cmd, sizeof(cmd))) return;

        char *p = cmd;
        while (isspace(*p)) p++;
        if (*p == '\0') {
            printf("[ERROR] Command cannot be empty.\n");
            continue;
        }
        memmove(cmd, p, strlen(p)+1);
        break;
    }

    int mode = ask_mode();

    /* Parse command tokens */
    char *argv[32];
    int argc = 0;

    char *tok = strtok(cmd, " ");
    while (tok && argc < 31) {
        argv[argc++] = strdup(tok);
        tok = strtok(NULL, " ");
    }
    argv[argc] = NULL;

    if (argc == 0) {
        printf("[ERROR] Invalid command.\n");
        return;
    }

    pid_t pid = fork();

    if (pid < 0) {
        perror("[ERROR] fork");
    }

    else if (pid == 0) {
        if (mode == MODE_DETACHED)
            setsid();

        execvp(argv[0], argv);
        perror("execvp");
        _exit(1);
    }

    else {
        ProcessInfo p;
        p.pid = pid;
        p.owner_pid = self_pid;
        p.mode = mode;
        p.status = STATUS_RUNNING;
        p.start_time = time(NULL);
        p.is_active = 1;

        char full[CMD_LEN] = {0};
        for (int i = 0; i < argc; ++i) {
            strcat(full, argv[i]);
            if (i+1 < argc) strcat(full, " ");
        }
        strncpy(p.command, full, CMD_LEN-1);

        if (add_proc(&p) == 0) {
            printf("[SUCCESS] Started PID %d\n", pid);
            send_mq(1, pid, p.command);
        }
    }

    for (int i = 0; i < argc; ++i) free(argv[i]);
}

static void terminate_process() {
    pid_t pid = ask_pid();
    if (pid <= 0) return;

    if (kill(pid, SIGTERM) == -1)
        perror("[WARN] kill");
    else {
        printf("[INFO] SIGTERM sent to %d\n", pid);
        send_mq(2, pid, "terminated");
    }
}


/* PRINT PROCESS LIST */

static void list_processes() {
    cleanup_dead_pids();

    sem_wait(gsem);
    printf("\n╔═══════════════════════════════════════════════════════════════╗\n");
    printf(  "║                    RUNNING PROGRAMS                           ║\n");
    printf(  "╠═══════════════════════════════════════════════════════════════╣\n");
    printf(  "║ PID   │ Command                    │ Mode     │ Owner │ Time  ║\n");
    printf(  "╠═══════════════════════════════════════════════════════════════╣\n");

    for (int i = 0; i < shm->process_count; ++i) {
        ProcessInfo *p = &shm->processes[i];
        if (p->is_active) {
            long t = time(NULL) - p->start_time;
            printf("║ %-5d│ %-26s│ %-8s│ %-5d│ %-5ld║\n",
                   p->pid,
                   p->command,
                   (p->mode == MODE_DETACHED ? "Detached" : "Attached"),
                   p->owner_pid,
                   t);
        }
    }

    printf(  "╚═══════════════════════════════════════════════════════════════╝\n");
    printf("Total: %d processes\n\n", shm->process_count);
    sem_post(gsem);
}


/* CLEANUP */

static void cleanup() {
    running = 0;

    pthread_cancel(monitor_thr);
    pthread_cancel(ipc_thr);
    pthread_join(monitor_thr, NULL);
    pthread_join(ipc_thr, NULL);

    /* Kendi başlattığımız attached process’leri öldürmek için yazıdı */
    sem_wait(gsem);
    for (int i = 0; i < shm->process_count; ++i) {
        ProcessInfo *p = &shm->processes[i];
        if (p->is_active && p->owner_pid == self_pid && p->mode == MODE_ATTACHED) {
            kill(p->pid, SIGTERM);
            p->is_active = 0;
        }
    }
    compact_list();
    sem_post(gsem);

    shmdt(shm);
    if (created_shm) shmctl(shmid, IPC_RMID, NULL);

    if (gsem) {
        sem_close(gsem);
        if (created_sem) sem_unlink(SEM_NAME);
    }

    if (created_mq) msgctl(mqid, IPC_RMID, NULL);

    printf("[INFO] ProcX exit complete.\n");
}


/*  Menü table */

static void sigint_handler(int sig) {
    (void)sig;
    running = 0;
}

int main() {
    self_pid = getpid();
    signal(SIGINT, sigint_handler);

    setup_ipc();

    pthread_create(&monitor_thr, NULL, monitor_thread, NULL);
    pthread_create(&ipc_thr, NULL, ipc_thread, NULL);

    while (running) {
        printf("\n╔══════════════════════════════╗\n");
        printf(  "║            ProcX             ║\n");
        printf(  "╠══════════════════════════════╣\n");
        printf(  "║ 1. Run new program           ║\n");
        printf(  "║ 2. List running programs     ║\n");
        printf(  "║ 3. Terminate program         ║\n");
        printf(  "║ 0. Exit                      ║\n");
        printf(  "╚══════════════════════════════╝\n");

        int c = ask_menu_choice(0, 3);
        if (!running) break;

        if (c == 1) start_process();
        else if (c == 2) list_processes();
        else if (c == 3) terminate_process();
        else if (c == 0) break;
    }

    cleanup();
    return 0;
}
