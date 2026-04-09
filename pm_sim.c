#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>

#define MAX_PROCESSES 64
#define MAX_LINE_LEN 256

typedef enum {
    EMPTY,
    RUNNING,
    BLOCKED,
    ZOMBIE,
    TERMINATED
} ProcessState;

typedef struct {
    int pid;
    int ppid;
    ProcessState state;
    int exit_status;
    pthread_cond_t cv;
} Process;

typedef struct {
    int thread_id;
    char filename[256];
} ThreadArgs;

// Global Variables
Process process_table[MAX_PROCESSES];
pthread_mutex_t table_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t monitor_cv = PTHREAD_COND_INITIALIZER;
pthread_cond_t monitor_ack_cv = PTHREAD_COND_INITIALIZER;

bool table_changed = false;
bool finished = false;
char last_action[512];
int next_pid = 2; // PID 1 is the initial process
FILE* snapshot_file;

// Helper function to convert state to string
const char* state_to_string(ProcessState state) {
    switch(state) {
        case RUNNING: return "RUNNING";
        case BLOCKED: return "BLOCKED";
        case ZOMBIE: return "ZOMBIE";
        case TERMINATED: return "TERMINATED";
        default: return "EMPTY";
    }
}

// Helper function to find a process index by PID
int get_process_index(int pid) {
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].state != EMPTY && process_table[i].pid == pid) {
            return i;
        }
    }
    return -1;
}

// Helper to notify the monitor thread to print
void notify_monitor(const char* action) {
    strncpy(last_action, action, sizeof(last_action));
    table_changed = true;
    pthread_cond_signal(&monitor_cv);
}

// --- CORE FUNCTIONS ---

void pm_fork(int parent_pid, int thread_id) {
    pthread_mutex_lock(&table_mutex);
    
    while (table_changed) {
        pthread_cond_wait(&monitor_ack_cv, &table_mutex);
    }

    int parent_idx = get_process_index(parent_pid);
    if (parent_idx == -1) {
        pthread_mutex_unlock(&table_mutex);
        return; 
    }

    int slot = -1;
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].state == EMPTY) {
            slot = i;
            break;
        }
    }

    if (slot != -1) {
        int new_pid = next_pid++;
        process_table[slot].pid = new_pid;
        process_table[slot].ppid = parent_pid;
        process_table[slot].state = RUNNING;
        process_table[slot].exit_status = 0;
        pthread_cond_init(&process_table[slot].cv, NULL);
        
        char action[256];
        sprintf(action, "Thread %d calls pm_fork %d", thread_id, parent_pid);
        notify_monitor(action);
    }

    pthread_mutex_unlock(&table_mutex);
}

void pm_exit(int pid, int status, int thread_id) {
    pthread_mutex_lock(&table_mutex);
    
    while (table_changed) {
        pthread_cond_wait(&monitor_ack_cv, &table_mutex);
    }
    
    int idx = get_process_index(pid);
    if (idx != -1 && process_table[idx].state != ZOMBIE) {
        process_table[idx].state = ZOMBIE;
        process_table[idx].exit_status = status;

        char action[256];
        sprintf(action, "Thread %d calls pm_exit %d %d", thread_id, pid, status);
        notify_monitor(action);

        int parent_idx = get_process_index(process_table[idx].ppid);
        if (parent_idx != -1) {
            pthread_cond_broadcast(&process_table[parent_idx].cv);
        }
    }
    
    pthread_mutex_unlock(&table_mutex);
}

void pm_wait(int parent_pid, int child_pid, int thread_id) {
    pthread_mutex_lock(&table_mutex);
    
    while (table_changed) {
        pthread_cond_wait(&monitor_ack_cv, &table_mutex);
    }
    
    int p_idx = get_process_index(parent_pid);
    if (p_idx == -1) {
        pthread_mutex_unlock(&table_mutex);
        return;
    }

    while (true) {
        bool has_child = false;
        int target_zombie_idx = -1;

        for (int i = 0; i < MAX_PROCESSES; i++) {
            if (process_table[i].state != EMPTY && process_table[i].state != TERMINATED) {
                if (process_table[i].ppid == parent_pid) {
                    if (child_pid == -1 || process_table[i].pid == child_pid) {
                        has_child = true;
                        if (process_table[i].state == ZOMBIE) {
                            target_zombie_idx = i;
                            break;
                        }
                    }
                }
            }
        }

        if (!has_child) {
            break; // No children to wait for
        }

        if (target_zombie_idx != -1) {
            process_table[target_zombie_idx].state = TERMINATED; 
            
            char action[256];
            sprintf(action, "Thread %d calls pm_wait %d %d", thread_id, parent_pid, child_pid);
            notify_monitor(action);
            break;
        } else {
            process_table[p_idx].state = BLOCKED;
            char action[256];
            sprintf(action, "Thread %d calls pm_wait %d %d (BLOCKED)", thread_id, parent_pid, child_pid);
            notify_monitor(action);

            pthread_cond_wait(&process_table[p_idx].cv, &table_mutex);

            // Parent woke up, must wait if monitor is busy before resuming
            while (table_changed) {
                pthread_cond_wait(&monitor_ack_cv, &table_mutex);
            }

            process_table[p_idx].state = RUNNING;
            sprintf(action, "Thread %d wakes up parent %d (RUNNING)", thread_id, parent_pid);
            notify_monitor(action);
        }
    }

    pthread_mutex_unlock(&table_mutex);
}

void pm_kill(int pid, int thread_id) {
    pthread_mutex_lock(&table_mutex);
    
    while (table_changed) {
        pthread_cond_wait(&monitor_ack_cv, &table_mutex);
    }
    
    int idx = get_process_index(pid);
    if (idx != -1 && process_table[idx].state != ZOMBIE) {
        process_table[idx].state = ZOMBIE;
        process_table[idx].exit_status = -1; // -1 indicates killed

        char action[256];
        sprintf(action, "Thread %d calls pm_kill %d", thread_id, pid);
        notify_monitor(action);

        int parent_idx = get_process_index(process_table[idx].ppid);
        if (parent_idx != -1) {
            pthread_cond_broadcast(&process_table[parent_idx].cv);
        }
    }
    
    pthread_mutex_unlock(&table_mutex);
}

// --- THREAD FUNCTIONS ---

void* monitor_thread_func(void* arg) {
    pthread_mutex_lock(&table_mutex);
    
    while (!finished || table_changed) {
        while (!table_changed && !finished) {
            pthread_cond_wait(&monitor_cv, &table_mutex);
        }
        
        if (table_changed) {
            fprintf(snapshot_file, "%s\n", last_action);
            fprintf(snapshot_file, "PID\t\tPPID\t\tSTATE\t\tEXIT_STATUS\n");
            fprintf(snapshot_file, "----------------------------------------------\n");
            
            for (int i = 0; i < MAX_PROCESSES; i++) {
                if (process_table[i].state != EMPTY && process_table[i].state != TERMINATED) {
                    if (process_table[i].state == ZOMBIE) {
                        fprintf(snapshot_file, "%d\t\t%d\t\t%s\t\t%d\n", 
                                process_table[i].pid, process_table[i].ppid, 
                                state_to_string(process_table[i].state), process_table[i].exit_status);
                    } else {
                        fprintf(snapshot_file, "%d\t\t%d\t\t%s\t\t-\n", 
                                process_table[i].pid, process_table[i].ppid, 
                                state_to_string(process_table[i].state));
                    }
                }
            }
            fprintf(snapshot_file, "\n");
            fflush(snapshot_file);
            
            table_changed = false;
            pthread_cond_broadcast(&monitor_ack_cv);
        }
    }
    
    pthread_mutex_unlock(&table_mutex);
    return NULL;
}

void* worker_thread_func(void* arg) {
    ThreadArgs* t_args = (ThreadArgs*)arg;
    FILE* file = fopen(t_args->filename, "r");
    
    if (!file) {
        return NULL;
    }

    char line[MAX_LINE_LEN];
    while (fgets(line, sizeof(line), file)) {
        char cmd[32];
        int arg1, arg2;
        
        if (sscanf(line, "%s", cmd) > 0) {
            if (strcmp(cmd, "fork") == 0) {
                sscanf(line, "%*s %d", &arg1);
                pm_fork(arg1, t_args->thread_id);
            } 
            else if (strcmp(cmd, "exit") == 0) {
                sscanf(line, "%*s %d %d", &arg1, &arg2);
                pm_exit(arg1, arg2, t_args->thread_id);
            } 
            else if (strcmp(cmd, "wait") == 0) {
                sscanf(line, "%*s %d %d", &arg1, &arg2);
                pm_wait(arg1, arg2, t_args->thread_id);
            } 
            else if (strcmp(cmd, "kill") == 0) {
                sscanf(line, "%*s %d", &arg1);
                pm_kill(arg1, t_args->thread_id);
            } 
            else if (strcmp(cmd, "sleep") == 0) {
                sscanf(line, "%*s %d", &arg1);
                usleep(arg1 * 1000); // convert milliseconds to microseconds
            }
        }
    }
    
    fclose(file);
    free(t_args);
    return NULL;
}

// --- MAIN FUNCTION ---

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: ./pm_sim <thread0.txt> <thread1.txt> ...\n");
        return 1;
    }

    snapshot_file = fopen("snapshots.txt", "w");
    if (!snapshot_file) {
        perror("Could not open snapshots.txt");
        return 1;
    }

    // Initialize process table
    for (int i = 0; i < MAX_PROCESSES; i++) {
        process_table[i].state = EMPTY;
    }

    // Create the initial init process (PID 1)
    process_table[0].pid = 1;
    process_table[0].ppid = 0;
    process_table[0].state = RUNNING;
    process_table[0].exit_status = 0;
    pthread_cond_init(&process_table[0].cv, NULL);

    // Write initial state to file immediately
    fprintf(snapshot_file, "Initial Process Table\n");
    fprintf(snapshot_file, "PID\t\tPPID\t\tSTATE\t\tEXIT_STATUS\n");
    fprintf(snapshot_file, "----------------------------------------------\n");
    fprintf(snapshot_file, "1\t\t0\t\tRUNNING\t\t-\n\n");
    fflush(snapshot_file);

    int num_worker_threads = argc - 1;
    pthread_t monitor_thread;
    pthread_t worker_threads[num_worker_threads];

    // Start monitor thread
    pthread_create(&monitor_thread, NULL, monitor_thread_func, NULL);

    // Start worker threads
    for (int i = 0; i < num_worker_threads; i++) {
        ThreadArgs* args = malloc(sizeof(ThreadArgs));
        args->thread_id = i;
        strncpy(args->filename, argv[i+1], sizeof(args->filename));
        pthread_create(&worker_threads[i], NULL, worker_thread_func, args);
    }

    // Wait for all worker threads to finish
    for (int i = 0; i < num_worker_threads; i++) {
        pthread_join(worker_threads[i], NULL);
    }

    // Signal monitor thread to finish and clean up
    pthread_mutex_lock(&table_mutex);
    finished = true;
    pthread_cond_signal(&monitor_cv);
    pthread_mutex_unlock(&table_mutex);

    pthread_join(monitor_thread, NULL);
    fclose(snapshot_file);

    printf("Simulation complete. Check snapshots.txt for the trace.\n");

    return 0;
}
