/* -------------------------------------------------------------------------- */
/*                                    PEER                                    */
/* -------------------------------------------------------------------------- */

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>

/* -------------------------------------------------------------------------- */
/*                                  COSTANTI                                  */
/* -------------------------------------------------------------------------- */
#define INPUT_LEN 48
#define BUFFER_LEN 256
#define ARG_LEN 32
#define NAME_REG_LEN 48
#define RECORD_LEN 32
#define N_ARG 5
#define POOLING_TIME 5
#define MAX_NEIGHBOR 2
/* -------------------------------------------------------------------------- */
/*                               STRUTTURE DATI                               */
/* -------------------------------------------------------------------------- */

struct resultCache
{
    char date[ARG_LEN];
    char type[ARG_LEN];
    int sum;
    struct resultCache *next;
};

/* -------------------------------------------------------------------------- */
/*                              VARIABILI GLOBALI                             */
/* -------------------------------------------------------------------------- */
// variabili per controllo comunicazione
int connected = 0;
int n_neighbor = 0;

// estremi dei registri
char *first_register_closed = "2021_01_01";
char last_register_closed[ARG_LEN];

struct resultCache *result_list = NULL;

// messaggi inviabili
const char *join = "JOIN";
/*  
    il messaggio di JOIN viene inviato dal peer al server per richiedere di 
    partecipare alla rete. 
*/
const char *quit = "QUIT";
/*
    Il messaggio di QUIT viene inviato dal peer al server per richiedere la
    disconnessione.
*/
const char *ask = "ASK";
/*
    Il messaggio di ASK viene inviato dal peer al server per chiedere quante entry
    di una determinata data e tipo mancano al richiedente.
*/
const char *ask_sum = "ASK_SUM";
/*
    Il messggio ASK_SUM viene inviato da un peer ad un altro per richiedere il dato
    mancante.
*/
const char *end = "END";
/*
    Il messaggio END viene inviato dal server a tutti i peer per segnalare la terminazione 
    del servizio. 
*/
const char *back = "BACK";
/*
    Il messaggio BACK viene inviato da un peer ad un altro per segnalre il cambiamento di vicino
*/
const char *next = "NEXT";
/*
    Il messaggio NEXT viene inviato da un peer ad un altro per segnalre il cambiamento di vicino
*/
const char *new_next = "NEW_NEXT";
/*
    Il messaggio NEW_NEXT viene inviato dal server ad un peer per segnalre il cambiamento di vicino
*/
const char *flood = "FLOOD_FOR_ENTRIES";
/*
    Il messaggio FLOOD_FOR_ENTRIES viene trasmesso tra i peer per richiedere i dati mancanti.
*/
const char *find = "FIND";
/*
    Il messaggio FIND viene inviato tra i peer per segnala al richiedente quali sono i peer che
    hanno il dato richiesto
*/
const char *req_data = "REQ_DATA";
/*
    Il messaggio REQ_DATA viene inviato da un peer ai sui vicini per chiedere se hanno gia'
    il dato aggregato richiesto salvato in cache.
*/
const char *transfer = "TRANSFER";
/*
    Messaggio che viene scambiato tra i peer per trasferire i register salvati prima della 
    chiusura del peer.
*/
const char *myAddr = "127.0.0.1";

/* -------------------------------------------------------------------------- */
/*                           FUNZIONI GESTIONE LISTE                          */
/* -------------------------------------------------------------------------- */

struct resultCache *insertResult(char *date, char *type, int sum)
{
    struct resultCache *newNode;
    newNode = malloc(sizeof(struct resultCache));
    if (newNode == NULL)
    {
        perror("Malloc non riuscita.\n");
        exit(EXIT_FAILURE);
    }

    strcpy(newNode->date, date);
    strcpy(newNode->type, type);
    newNode->sum = sum;

    newNode->next = result_list;
    result_list = newNode;
}

struct resultCache *searchResult(char *date, char *type)
{
    if (result_list == NULL)
        return NULL;
    struct resultCache *curr = result_list;
    while (curr->next != NULL)
    {
        if (strcmp(curr->date, date) == 0 && strcmp(curr->type, type) == 0)
            return curr;
        curr = curr->next;
    }
    return NULL;
}

void deleteAll()
{
    struct resultCache *curr, *temp;
    curr = result_list;
    temp = NULL;
    while (curr != NULL)
    {
        temp = curr->next;
        free(curr);
        curr = temp;
    }
    result_list = NULL;
}
/* -------------------------------------------------------------------------- */
/*                            FUNZIONI DI SERVIZIO                            */
/* -------------------------------------------------------------------------- */

void userMessage(char *port)
{
    //messaggio iniziale
    printf("********************************* PEER COVID STARTED ********************************* \n");
    printf("Digita un comando (%s):\n\n", port);
    printf("1) start <DS_addr> <DS_port> --> chiede la registrazione ad un DS.\n");
    printf("2) add <type> <quantity> --> aggiunge al register della data corrente le informazioni inserite.\n");
    printf("3) get <aggr> <type> <period> --> restituisce il dato aggregato richiesto nel relativo lasso di tempo.\n");
    printf("4) stop --> termina l'esecuzione del peer.\n\n");
}
// calcola fino a quale register puo' essere effettuata un' interazione
void lastRegisterClosed()
{
    time_t rawtime;
    struct tm *timeinfo;

    time(&rawtime);
    timeinfo = localtime(&rawtime);

    if (timeinfo->tm_hour < 18)
    {
        // ieri
        rawtime -= (24 * 60 * 60);
        timeinfo = localtime(&rawtime);
        sprintf(last_register_closed, "%d_%d_%d", timeinfo->tm_year + 1900, timeinfo->tm_mon + 1, timeinfo->tm_mday);
    }
    else
        sprintf(last_register_closed, "%d_%d_%d", timeinfo->tm_year + 1900, timeinfo->tm_mon + 1, timeinfo->tm_mday);
}

// incrementa la data
void incrementDate(char *date)
{
    int year, mon, day;
    int isLeap = 0;
    time_t rawtime;
    struct tm *timeinfo;

    // estraggo data
    sscanf(date, "%d_%d_%d", &year, &mon, &day);

    time(&rawtime);
    timeinfo = localtime(&rawtime);
    timeinfo->tm_year = year - 1900;
    timeinfo->tm_mon = mon - 1;
    timeinfo->tm_mday = day;

    rawtime = mktime(timeinfo);
    rawtime += (24 * 60 * 60);
    timeinfo = localtime(&rawtime);

    sprintf(date, "%d_%d_%d", timeinfo->tm_year + 1900, timeinfo->tm_mon + 1, timeinfo->tm_mday);
    return;
}

/* -------------------------------------------------------------------------- */
/*                           FUNZIONI DI VALIDAZIONE                          */
/* -------------------------------------------------------------------------- */

int validateType(char type[ARG_LEN])
{
    if (strcmp(type, "tampone") != 0 && strcmp(type, "positivo") != 0)
    {
        printf("Tipo inserito non valido.\n");
        return 0;
    }
    return 1;
}

int validateAggr(char aggr[ARG_LEN])
{
    if (strcmp(aggr, "totale") != 0 && strcmp(aggr, "variazione") != 0)
    {
        printf("Data aggregato richiesto errato.\n");
        return 0;
    }
    return 1;
}

int isValidNum(char *port)
{
    int num = 0;
    if (sscanf(port, "%d", &num) != 1)
        return 0;

    if (num <= 0 || num > 1000)
        return 0;
    return num;
}

int dateValidation(int day, int mon, int year)
{
    int isLeap = 0;
    int isValid = 1;

    if (year >= 2020 && year <= 9999)
    {
        // controllo bisestile
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))
            isLeap = 1;

        // controllo se valida
        if (mon >= 1 && mon <= 12)
        {
            if (mon == 2)
            {
                if (day > 28)
                    isValid = 0;
                if (isLeap && day == 29)
                    isValid = 1;
            }
            else if (mon == 4 || mon == 6 || mon == 9 || mon == 11)
            {
                if (day > 30)
                    isValid = 0;
            }
            else
            {
                if (day > 31)
                    isValid = 0;
            }
        }
        else
            isValid = 0;
    }
    else
        isValid = 0;

    return isValid;
}

// conta le entry di una daterminata data e tipo in un register
int countRecord(char name[NAME_REG_LEN], char type[ARG_LEN])
{
    // variabili di appoggio
    char line[RECORD_LEN];
    int offset = 0;
    int num = 0;
    int value;
    char tempDate[ARG_LEN];
    char tempType[ARG_LEN];

    // apertura file
    FILE *fp = fopen(name, "r");
    if (fp == NULL)
        return 0;

    // scorro i record del file
    while (fgets(line, sizeof(line), fp) != NULL)
    {
        // leggo il record
        sscanf(line, "%s %s %d", tempDate, tempType, &value);
        if (strcmp(tempType, type) == 0)
            num++;

        // scorro il record
        offset += strlen(line);
        fseek(fp, offset, SEEK_SET);
    }
    // chiudo il file
    fclose(fp);

    printf("Ho trovato %d record del tipo %s nel registro %s\n", num, type, name);
    return num;
}
// conta la somma dei pazienti di una daterminata data e tipo in un register
int sumRecord(char name[NAME_REG_LEN], char type[ARG_LEN])
{
    // variabili di appoggio
    char line[RECORD_LEN];
    int offset = 0;
    int sum = 0;
    int value;
    char tempDate[ARG_LEN];
    char tempType[ARG_LEN];

    // apro il file
    FILE *fp = fopen(name, "r");
    if (fp == NULL)
        return 0;

    // scorro i record del file
    while (fgets(line, sizeof(line), fp) != NULL)
    {
        // controlli ogni singolo record
        sscanf(line, "%s %s %d", tempDate, tempType, &value);
        if (strcmp(tempType, type) == 0)
            sum += value;

        // vado al record successivo
        offset += strlen(line);
        fseek(fp, offset, SEEK_SET);
    }
    // chiudo il file
    fclose(fp);

    printf("Ho trovato %d del tipo %s nel registro %s\n", sum, type, name);
    return sum;
}
// scompone le date inserite in un range
int parseDate(char *target, char *startDate, char *endDate)
{
    // variabili di appoggio
    time_t rawtime, secStart, secFinish;
    struct tm *today, *start, *finish;
    int day1, mon1, year1, day2, mon2, year2;

    // prendo la data corrente
    time(&rawtime);
    today = localtime(&rawtime);

    // scompongo la data
    if (sscanf(target, "%02i:%02i:%04i-%02i:%02i:%04i", &day1, &mon1, &year1, &day2, &mon2, &year2) != 6)
        return 0;

    // verifico la validita' delle date estratte
    if (dateValidation(day1, mon1, year1) == 0 || dateValidation(day2, mon2, year2) == 0)
        return 0;

    // verifico che si segua un ordine cronologico
    if (year1 > year2 || (year1 == year2 && mon1 > mon2) || (year1 == year2 && mon1 == mon2 && day1 > day2))
        return 0;

    // salvo le due date estratte
    sprintf(startDate, "%i_%i_%i", year1, mon1, day1);
    sprintf(endDate, "%i_%i_%i", year2, mon2, day2);

    return 1;
}
/* -------------------------------------------------------------------------- */
/*                                    MAIN                                    */
/* -------------------------------------------------------------------------- */

int main(int argc, char *argv[])
{

    /* --------------------------- vaibili per socket --------------------------- */
    int ret, sd_udp, sd_tcp, new_sd, next_sd, back_sd, sd_srv, addrlen, len, fd_max;
    struct sockaddr_in srv_addr, my_addr, back_neighbor_addr, next_neighbor_addr, connecting_addr;
    fd_set current_socket, ready_socket;
    char buffer[BUFFER_LEN];
    char input[INPUT_LEN];
    char args[N_ARG][ARG_LEN];
    int n_arg = 0;
    int enable = 1;

    // socket dei vicini non utilizzati
    back_sd = -1;
    next_sd = -1;

    // inizializzo select
    FD_ZERO(&current_socket);
    FD_ZERO(&ready_socket);

    // creo il mio socket
    sd_udp = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
    sd_tcp = socket(AF_INET, SOCK_STREAM, 0);

    if (setsockopt(sd_udp, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
    {
        perror("Errore in fase di setsockopt\n");
        exit(EXIT_FAILURE);
    }
    if (setsockopt(sd_tcp, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
    {
        perror("Errore in fase di setsockopt\n");
        exit(EXIT_FAILURE);
    }

    // preparo il mio indirizzo del socket
    memset(&my_addr, 0, sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(atoi(argv[1]));
    my_addr.sin_addr.s_addr = INADDR_ANY;

    // collego il socket al suo indirizzo
    ret = bind(sd_udp, (struct sockaddr *)&my_addr, sizeof(my_addr));
    if (ret < 0)
    {
        perror("Bind non riuscita");
        exit(EXIT_FAILURE);
    }
    ret = bind(sd_tcp, (struct sockaddr *)&my_addr, sizeof(my_addr));
    if (ret < 0)
    {
        perror("Bind non riuscita");
        exit(EXIT_FAILURE);
    }
    listen(sd_tcp, 10);

    // inizializzo la select
    FD_SET(0, &current_socket);
    FD_SET(sd_udp, &current_socket);
    FD_SET(sd_tcp, &current_socket);
    if (sd_tcp > sd_udp)
        fd_max = sd_tcp;
    else
        fd_max = sd_udp;

    // messaggio iniziale
    userMessage(argv[1]);

    /* -------------------------------------------------------------------------- */
    /*                                   DEMONE                                   */
    /* -------------------------------------------------------------------------- */
    while (1)
    {
        // operazioni sulla select
        int fd_curr;
        ready_socket = current_socket;
        select(fd_max + 1, &ready_socket, NULL, NULL, NULL);

        for (fd_curr = 0; fd_curr <= fd_max; fd_curr++)
        {
            if (FD_ISSET(fd_curr, &ready_socket))
            {
                if (fd_curr == 0)
                {
                    /* -------------------------------------------------------------------------- */
                    /*                                    STDIN                                   */
                    /* -------------------------------------------------------------------------- */
                    // prendo gli input da tastiera
                    fgets(input, sizeof(input), stdin);
                    n_arg = sscanf(input, "%s %s %s %s", args[0], args[1], args[2], args[3]);

                    //controllo del comando inserito
                    if (strcmp(args[0], "start") == 0 && n_arg == 3) // start 127.0.0.1 4242
                    {
                        /* ---------------------------------- START --------------------------------- */
                        // controllo se sono gia' connesso
                        if (connected == 1)
                        {
                            printf("Deve prima avvenire una disconnesione.\n");
                            continue;
                        }

                        //inserisco indirizzo del DS server
                        memset(&srv_addr, 0, sizeof(srv_addr));
                        srv_addr.sin_family = AF_INET;
                        srv_addr.sin_port = htons(atoi(args[2]));
                        inet_pton(AF_INET, args[1], &srv_addr.sin_addr);

                        // ciclo di connessione
                        while (connected == 0)
                        {
                            // varibili di appoggio
                            int next_port, back_port;
                            int attempt = 0;
                            char next_addr[ARG_LEN];
                            char back_addr[ARG_LEN];
                            char cmd[ARG_LEN];

                            // chiedo il join
                            printf("Connessione al DS server...\n");
                            sprintf(buffer, "%s %s %s", join, myAddr, argv[1]);
                            do
                            {
                                ret = sendto(sd_udp, buffer, BUFFER_LEN, 0, (struct sockaddr *)&srv_addr, sizeof(srv_addr));
                                if (ret < 0)
                                    sleep(POOLING_TIME);
                            } while (ret < 0);

                            // attendo l'update
                            do
                            {
                                ret = recvfrom(sd_udp, buffer, BUFFER_LEN, 0, (struct sockaddr *)&connecting_addr, &len);
                                if (ret < 0)
                                {
                                    attempt++;
                                    if (attempt > 3)
                                        continue;
                                    sleep(POOLING_TIME);
                                }
                            } while (ret < 0);

                            // controllo il formato del messaggio ricevuto
                            n_arg = sscanf(buffer, "%s %s %d %s %d", cmd, back_addr, &back_port, next_addr, &next_port);
                            if (n_arg == 0 && strcmp(cmd, "UPDATE") != 0)
                            {
                                printf("Messaggio non valido.\n");
                                continue;
                            }

                            printf("Aggiorno il vicinato.\n");
                            // controllo i vicini ricevuti
                            if (n_arg >= 3)
                            {
                                // aumento il numero di vicini
                                n_neighbor++;

                                // salvo l'indirizzo del vicino back
                                back_sd = socket(AF_INET, SOCK_STREAM, 0);
                                memset(&back_neighbor_addr, 0, sizeof(back_neighbor_addr));
                                back_neighbor_addr.sin_family = AF_INET;
                                back_neighbor_addr.sin_port = htons(back_port);
                                inet_pton(AF_INET, back_addr, &back_neighbor_addr.sin_addr);

                                // mi connetto al nuovo vicino
                                ret = connect(back_sd, (struct sockaddr *)&back_neighbor_addr, sizeof(back_neighbor_addr));
                                if (ret < 0)
                                {
                                    perror("Errore in fase di connect");
                                    continue;
                                }

                                // invio le mie informazioni al nuovo vicino
                                sprintf(buffer, "%s %s %s", next, myAddr, argv[1]);
                                ret = send(back_sd, (void *)buffer, BUFFER_LEN, 0);
                                if (ret < 0)
                                {
                                    perror("Errore in fase di send");
                                    continue;
                                }

                                // aggiungo il nuovo vicino alla select
                                FD_SET(back_sd, &current_socket);
                                if (back_sd > fd_max)
                                    fd_max = back_sd;

                                printf("Il mio vicino back e': %d\n", back_port);

                                if (n_arg == 5)
                                {
                                    // aumento il numero di vicini
                                    n_neighbor++;

                                    // salvo l'indirizzo del nuovo vicino
                                    next_sd = socket(AF_INET, SOCK_STREAM, 0);
                                    memset(&next_neighbor_addr, 0, sizeof(next_neighbor_addr));
                                    next_neighbor_addr.sin_family = AF_INET;
                                    next_neighbor_addr.sin_port = htons(next_port);
                                    inet_pton(AF_INET, next_addr, &next_neighbor_addr.sin_addr);

                                    // mi connetto al nuovo vicino
                                    ret = connect(next_sd, (struct sockaddr *)&next_neighbor_addr, sizeof(next_neighbor_addr));
                                    if (ret < 0)
                                    {
                                        perror("Errore in fase di connect");
                                        continue;
                                    }

                                    // invio le mie informazioni al nuovo vicino
                                    sprintf(buffer, "%s %s %s", back, myAddr, argv[1]);
                                    ret = send(next_sd, (void *)buffer, BUFFER_LEN, 0);
                                    if (ret < 0)
                                    {
                                        perror("Errore in fase di send");
                                        continue;
                                    }

                                    // aggiungo il peer nella select
                                    FD_SET(next_sd, &current_socket);
                                    if (next_sd > fd_max)
                                        fd_max = next_sd;

                                    printf("Il mio vicino next e': %d\n", next_port);
                                }
                            }
                            // posso uscire dal loop
                            connected = 1;
                        }
                        // confermo la connessione
                        printf("Connessione riuscita.\n");
                    }
                    else if (strcmp(args[0], "add") == 0 && n_arg == 3) // add tipo num
                    {
                        /* ----------------------------------- ADD ---------------------------------- */
                        // variabili di appoggio
                        int sd_srv;
                        int numEntry = 0;
                        char nameRegister[NAME_REG_LEN];
                        char currentDate[ARG_LEN];
                        char newRecord[ARG_LEN];
                        time_t rawtime;
                        FILE *newRegister;
                        struct stat st = {0};
                        struct tm *timeinfo;

                        // verifico di essere prima connesso
                        if (connected == 0)
                        {
                            printf("Il peer non e' connesso ad alcun DS server.\n");
                            continue;
                        }

                        // controllo validita' del tipo
                        if (!validateType(args[1]))
                            continue;

                        // controllo quantita' inserita
                        numEntry = isValidNum(args[2]);
                        if (numEntry == 0)
                        {
                            printf("Quantita' inserita non valida.\n");
                            continue;
                        }

                        // prendo la data corrente
                        time(&rawtime);
                        timeinfo = localtime(&rawtime);

                        //controllo se chiudere il giorno
                        if (timeinfo->tm_hour > 18 || (timeinfo->tm_hour == 18 && timeinfo->tm_min > 0) || (timeinfo->tm_hour == 18 && timeinfo->tm_min == 0 && timeinfo->tm_sec > 0))
                        {
                            // inizio a riempire il giorno successivo e chiudo il precedente
                            sprintf(last_register_closed, "%d_%d_%d", timeinfo->tm_year + 1900, timeinfo->tm_mon + 1, timeinfo->tm_mday);
                            rawtime += (60 * 60 * 24);
                            timeinfo = localtime(&rawtime);
                        }
                        sprintf(currentDate, "%d_%d_%d", timeinfo->tm_year + 1900, timeinfo->tm_mon + 1, timeinfo->tm_mday);

                        // controllo se la directory esiste altrimenti la creo
                        sprintf(nameRegister, "./Register%s", argv[1]);
                        if (stat(nameRegister, &st) == -1)
                            mkdir(nameRegister, 0777);

                        // prendo il nome del register da aprire
                        sprintf(nameRegister, "./Register%s/Register_%s_%s.txt", argv[1], argv[1], currentDate);

                        // apro il file in scrittura
                        newRegister = fopen(nameRegister, "a");
                        if (newRegister == NULL)
                        {
                            printf("Errore nell' apertura del Register.\n");
                            continue;
                        }

                        // creazione del record
                        sprintf(newRecord, "%s %s %s\n", currentDate, args[1], args[2]);

                        // scrivo il record e chiudo il file
                        fprintf(newRegister, "%s", newRecord);
                        fclose(newRegister);

                        // invio menu opzioni
                        userMessage(argv[1]);
                    }
                    else if (strcmp(args[0], "get") == 0 && n_arg >= 3) // get tipoAggr tipo periodo
                    {
                        /* ----------------------------------- GET ---------------------------------- */
                        struct resultCache *target = NULL;
                        char startDate[ARG_LEN];
                        char endDate[ARG_LEN];
                        char currDate[ARG_LEN];
                        char *noBack;
                        char *noFront;
                        int total = 0;
                        int year, mon, day;

                        // controllo di essere connesso
                        if (connected == 0)
                        {
                            printf("Si deve prima connettersi al DS server.\n");
                            continue;
                        }

                        // controllo il valore aggregato
                        if (!validateAggr(args[1]))
                            continue;

                        // controllo il tipo
                        if (!validateType(args[2]))
                            continue;

                        //calcolo ultimo register chiuso
                        lastRegisterClosed();

                        //verifico presenza di caratteri speciali
                        noBack = strstr(args[3], "*-");
                        noFront = strstr(args[3], "-*");

                        // verifiche sul periodo
                        if (n_arg == 3)
                        {
                            //tutto il periodo
                            strcpy(startDate, first_register_closed);
                            strcpy(endDate, last_register_closed);
                        }
                        else if (noBack != NULL)
                        {
                            //mancano il bordo inferiore
                            strcpy(startDate, first_register_closed);
                            if (sscanf(args[3], "*-%02i:%02i:%04i", &day, &mon, &year) != 3 || !dateValidation(day, mon, year))
                            {
                                printf("Periodo inserito non valido.\n");
                                continue;
                            }
                            sprintf(endDate, "%d_%d_%d", year, mon, day);
                        }
                        else if (noFront != NULL)
                        {
                            //manca il bordo superiore
                            strcpy(endDate, last_register_closed);
                            if (sscanf(args[3], "%02i:%02i:%04i-*", &day, &mon, &year) != 3 || !dateValidation(day, mon, year))
                            {
                                printf("Periodo inserito non valido.\n");
                                continue;
                            }
                            sprintf(startDate, "%d_%d_%d", year, mon, day);
                        }
                        else
                        {
                            // periodo specifico
                            if (parseDate(args[3], startDate, endDate) == 0)
                            {
                                printf("Periodo inserito non valido.\n");
                                continue;
                            }
                        }

                        //ciclo sul periodo
                        int precedente = 0;
                        strcpy(currDate, startDate);
                        incrementDate(endDate);
                        while (strcmp(currDate, endDate) != 0)
                        {
                            // variabili di appoggio
                            char registerName[NAME_REG_LEN];
                            int missingRecord = 0;
                            int n_peer;

                            printf("Cerco i dati del giorno %s\n", currDate);
                            sprintf(registerName, "./Register%s/Register_%s_%s.txt", argv[1], argv[1], currDate);
                            // cerco il dato in cache
                            target = searchResult(currDate, args[2]);
                            if (target != NULL)
                            {
                                // aggiorno la somma
                                total += target->sum;
                                printf("Ho il dato salvato in cache.\n");

                                //esprimo la variazione
                                if (strcmp(args[1], "totale") != 0)
                                {
                                    printf("La variazione del %s e': %d\n", currDate, (target->sum - precedente));
                                    precedente = target->sum;
                                }

                                // prossima iterazione
                                incrementDate(currDate);
                                continue;
                            }

                            // invio il messaggio al server
                            sd_srv = socket(AF_INET, SOCK_STREAM, 0);
                            ret = connect(sd_srv, (struct sockaddr *)&srv_addr, sizeof(srv_addr));
                            if (ret < 0)
                            {
                                perror("Errore in fase di connect");
                                continue;
                            }

                            // preparo il messaggio per il server
                            sprintf(buffer, "%s %s %s %s", ask, argv[1], currDate, args[2]);
                            ret = send(sd_srv, (void *)buffer, BUFFER_LEN, 0);
                            if (ret < 0)
                            {
                                perror("Errore in fase di send");
                                continue;
                            }

                            // attendo la risposta del server
                            ret = recv(sd_srv, (void *)buffer, BUFFER_LEN, 0);
                            if (ret < 0)
                            {
                                perror("Errore in fase di recv");
                                continue;
                            }

                            // osservo la risposta
                            if (strcmp("OK", buffer) == 0)
                            {
                                // calcolo il risultato e lo aggiungo al totale
                                int sum = sumRecord(registerName, args[2]);
                                total += sum;

                                // esprimo la variazione
                                if (strcmp(args[1], "totale") != 0)
                                {
                                    printf("La variazione del %s e': %d\n", currDate, (sum - precedente));
                                    precedente = sum;
                                }

                                // prossima iterazio
                                insertResult(currDate, args[2], sum);
                                incrementDate(currDate);
                                continue;
                            }
                            else
                            {
                                // non posso calcolare il risultato da solo
                                if (sscanf(buffer, "MISSING %d %d", &missingRecord, &n_peer) != 2)
                                    printf("Formato non corretto.\n");
                            }

                            // chiudo connessione con il server
                            close(sd_srv);

                            //chiedo ai vicini se hanno il dato in cache
                            if (next_sd != -1)
                            {
                                char response[ARG_LEN];
                                int sum = 0;

                                printf("Chiedo ai vicini se hanno il dato in cache del %s.\n", currDate);

                                // costruisco il messaggio
                                sprintf(buffer, "%s %s %s", req_data, args[2], currDate);

                                //chiedo al vicino next se ha il dato in cache
                                ret = send(next_sd, (void *)buffer, BUFFER_LEN, 0);
                                if (ret < 0)
                                {
                                    perror("Errore in fase di send");
                                    continue;
                                }

                                // attendo risposta
                                ret = recv(next_sd, (void *)buffer, BUFFER_LEN, 0);
                                if (ret < 0)
                                {
                                    perror("Errore in fase di recv");
                                    continue;
                                }

                                // controllo se il vicino ha il risultato
                                if (sscanf(buffer, "%s %i", response, &sum) == 2 && strcmp(response, "REPLAY_DATA") == 0)
                                {
                                    // salvo le informazioni
                                    total += sum;
                                    if (strcmp(args[1], "totale") != 0)
                                    {
                                        printf("La variazione del %s e': %d\n", currDate, (sum - precedente));
                                        precedente = sum;
                                    }

                                    // prossima iterazione
                                    insertResult(currDate, args[2], sum);
                                    incrementDate(currDate);
                                    continue;
                                }
                            }

                            // invio anche al secondo vicino
                            if (back_sd != -1)
                            {
                                char response[ARG_LEN];
                                int sum = 0;

                                // creo il messaggio
                                sprintf(buffer, "%s %s %s", req_data, args[2], currDate);

                                //chiedo al vicino back
                                ret = send(back_sd, (void *)buffer, BUFFER_LEN, 0);
                                if (ret < 0)
                                {
                                    perror("Errore in fase di send");
                                    continue;
                                }

                                // attendo risposta del vicino back
                                ret = recv(back_sd, (void *)buffer, BUFFER_LEN, 0);
                                if (ret < 0)
                                {
                                    perror("Errore in fase di recv");
                                    continue;
                                }

                                // controllo se il vicino ha il risultato
                                if (sscanf(buffer, "%s %i", response, &sum) == 2 && strcmp(response, "REPLAY_DATA") == 0)
                                {
                                    // salvo le informazioni per il totale
                                    total += sum;

                                    // comunico la variazione
                                    if (strcmp(args[1], "totale") != 0)
                                    {
                                        printf("La variazione del %s e': %d\n", currDate, (sum - precedente));
                                        precedente = sum;
                                    }

                                    // prossima iterazione
                                    insertResult(currDate, args[2], sum);
                                    incrementDate(currDate);
                                    continue;
                                }
                            }

                            // preparo un vettore per salvare le porte
                            int index = 0;
                            int i;
                            int *vettTarget = (int *)malloc(missingRecord * sizeof(int));
                            if (vettTarget == NULL)
                            {
                                perror("Errore causato da malloc.\n");
                                exit(EXIT_FAILURE);
                            }

                            // invio il messaggio in avanti
                            if (n_neighbor > 0)
                            {
                                //costruisco il messaggio di flood
                                sprintf(buffer, "%s %s %s %d %s", flood, currDate, args[2], n_peer, argv[1]);

                                //faccio partire il flood da next
                                ret = send(next_sd, (void *)buffer, BUFFER_LEN, 0);
                                if (ret < 0)
                                {
                                    perror("Errore in fase di send");
                                    continue;
                                }

                                // aspetto tutte le find che mi mancano
                                while (missingRecord > 0)
                                {
                                    int target_port = 0;
                                    int check_port = 0;

                                    printf("Devo ricevere %d record del giorno %s.\n", missingRecord, currDate);

                                    //mi metto in attesa di risposte
                                    ret = recv(next_sd, (void *)buffer, BUFFER_LEN, 0);
                                    if (ret < 0)
                                    {
                                        perror("Errore in fase di recv");
                                        continue;
                                    }

                                    // controllo il messaggio
                                    if (sscanf(buffer, "FIND %d %d", &target_port, &check_port) == 2 && check_port == atoi(argv[1]))
                                    {
                                        // salvare il target nel vettore
                                        *(vettTarget + index) = target_port;
                                        index++;

                                        //decrementare il numero di missing
                                        missingRecord--;
                                    }
                                    else if (sscanf(buffer, "FIND %d %d", &target_port, &check_port) == 2 && check_port != atoi(argv[1]))
                                    {
                                        ret = send(back_sd, (void *)buffer, BUFFER_LEN, 0);
                                        if (ret < 0)
                                        {
                                            perror("Errore in fase di send");
                                            continue;
                                        }
                                    }
                                }
                            }

                            // chiedo il dato ai target salvati nel vettore
                            int tot_partial = 0;
                            for (i = 0; i < index; i++)
                            {
                                int missing_sum = 0;
                                int sd_target = socket(AF_INET, SOCK_STREAM, 0);

                                // prendo indirizzo dal vettore
                                struct sockaddr_in target_addr;
                                memset(&target_addr, 0, sizeof(target_addr));
                                target_addr.sin_family = AF_INET;
                                target_addr.sin_port = htons(*vettTarget + i);
                                inet_pton(AF_INET, "127.0.0.1", &target_addr.sin_addr);

                                // mi connetto al target
                                ret = connect(sd_target, (struct sockaddr *)&target_addr, sizeof(target_addr));
                                if (ret < 0)
                                {
                                    perror("Errore in fase di connect");
                                    continue;
                                }

                                // invio richiesta di dati
                                sprintf(buffer, "%s %s %s", ask_sum, currDate, args[2]);
                                ret = send(sd_target, (void *)buffer, BUFFER_LEN, 0);
                                if (ret < 0)
                                {
                                    perror("Errore in fase di send");
                                    continue;
                                }

                                // attendo la risposta
                                ret = recv(sd_target, (void *)buffer, BUFFER_LEN, 0);
                                if (ret < 0)
                                {
                                    perror("Errore in fase di recv");
                                    continue;
                                }

                                //estraggo somma
                                if (sscanf(buffer, "RESPONSE_SUM %d", &missing_sum) != 1)
                                {
                                    printf("Formato non valido.\n");
                                    continue;
                                }

                                //calcolo le somme parziali
                                tot_partial += missing_sum;

                                // chiudo connessione con il target
                                close(sd_target);
                            }
                            //libero il vettore
                            free(vettTarget);

                            // sommo tutti i dati che mi sono stati inviati ed aggingo il mio
                            total += tot_partial + sumRecord(registerName, args[2]);

                            // scrivo la variazione
                            if (strcmp(args[1], "totale") != 0)
                            {
                                printf("La variazione del %s e': %d\n", currDate, (tot_partial - precedente + sumRecord(registerName, args[2])));
                                precedente = tot_partial + sumRecord(registerName, args[2]);
                            }

                            // prossima iterazione
                            insertResult(currDate, args[2], (tot_partial + sumRecord(registerName, args[2])));
                            incrementDate(currDate);
                        } // END WHILE
                        // calcolo del totale
                        if (strcmp(args[1], "totale") == 0)
                            printf("Il totale e': %d\n", total);

                        userMessage(argv[1]);
                    }
                    else if (strcmp(args[0], "stop") == 0 && n_arg == 1)
                    {
                        /* ---------------------------------- STOP ---------------------------------- */
                        int neighborTarget = 0;
                        char dirName[MAXNAMLEN];

                        sprintf(dirName, "./Register%s/", argv[1]);

                        // scelgo il vicino
                        if (back_sd != -1)
                            neighborTarget = back_sd;
                        else if (next_sd != -1)
                            neighborTarget = next_sd;

                        // controllo se ho vicini a cui trasferire i record
                        if (neighborTarget != 0)
                        {
                            // vaeibili per gestione directory
                            struct dirent *files;

                            //provo ad aprire la directory
                            DIR *dir = opendir(dirName);
                            if (dir == NULL)
                            {
                                printf("Errore in fase di apertura della directory.\n");
                                continue;
                            }

                            // scorro tutti i file della directory
                            while ((files = readdir(dir)) != NULL)
                            {
                                // variabili di appoggio
                                int offset = 0;
                                int num = 0;
                                int value;
                                char fileName[FILENAME_MAX];
                                char line[RECORD_LEN];
                                char tempDate[ARG_LEN];
                                char tempType[ARG_LEN];
                                FILE *fptr;

                                // scrivo il path del Register
                                strcpy(fileName, dirName);
                                strcat(fileName, files->d_name);

                                // apro il register in lettura
                                fptr = fopen(fileName, "r");
                                if (fptr == NULL)
                                {
                                    perror("Errore in fase di apertura file");
                                    continue;
                                }

                                //scorro i record del file
                                while (fgets(line, sizeof(line), fptr) != NULL)
                                {
                                    // legge il record
                                    sscanf(line, "%s %s %d", tempDate, tempType, &value);

                                    //invio al vicino il record
                                    sprintf(buffer, "%s %s %s %d", transfer, tempDate, tempType, value);
                                    ret = send(neighborTarget, (void *)buffer, BUFFER_LEN, 0);
                                    if (ret < 0)
                                    {
                                        perror("Errore in fase di send");
                                        continue;
                                    }

                                    // scorro al record successivo
                                    offset += strlen(line);
                                    fseek(fptr, offset, SEEK_SET);
                                }

                                //chiudo il file e lo eleimino
                                fclose(fptr);
                                remove(fileName);
                            }
                            //chiudo la directory
                            closedir(dir);
                            printf("Ho finito di trasferire i miei dati.\n");
                        }

                        // mi connetto al server
                        sd_srv = socket(AF_INET, SOCK_STREAM, 0);
                        ret = connect(sd_srv, (struct sockaddr *)&srv_addr, sizeof(srv_addr));
                        if (ret < 0)
                        {
                            perror("Errore in fase di connect");
                            continue;
                        }

                        //invio al server la richiesta di disconnessione
                        sprintf(buffer, "%s %s", quit, argv[1]);
                        ret = send(sd_srv, (void *)buffer, BUFFER_LEN, 0);
                        if (ret < 0)
                        {
                            perror("Errore in fase di send");
                            continue;
                        }

                        // chiudo la connessione
                        printf("Chiudo la connessione.\n");
                        close(sd_srv);

                        connected = 0;
                        if (next_sd != -1)
                        {
                            FD_CLR(next_sd, &current_socket);
                            close(next_sd);
                            next_sd = -1;
                        }

                        if (back_sd != -1)
                        {
                            FD_CLR(back_sd, &current_socket);
                            close(back_sd);
                            back_sd = -1;
                        }

                        userMessage(argv[1]);
                    }
                    else
                        printf("Comando non valido.\n");
                }
                else if (fd_curr == sd_tcp)
                {
                    /* -------------------------------------------------------------------------- */
                    /*                                  LISTENER                                  */
                    /* -------------------------------------------------------------------------- */
                    addrlen = sizeof(connecting_addr);

                    // accetto la richiesta del peer
                    new_sd = accept(sd_tcp, (struct sockaddr *)&connecting_addr, &addrlen);
                    FD_SET(new_sd, &current_socket);
                    if (new_sd > fd_max)
                        fd_max = new_sd;
                }
                else
                {
                    /* -------------------------------------------------------------------------- */
                    /*                           PEER DISTANTI o SERVER                           */
                    /* -------------------------------------------------------------------------- */

                    // ricevo un messaggio
                    int n_arg;
                    char args[N_ARG][ARG_LEN];

                    // ricevo il messaggio di richiesta
                    ret = recv(fd_curr, (void *)buffer, BUFFER_LEN, 0);
                    if (ret < 0)
                    {
                        perror("Errore in fase di recv");
                        goto clean;
                    }

                    // controllo tipo di richiesta
                    n_arg = sscanf(buffer, "%s %s %s %s %s", args[0], args[1], args[2], args[3], args[4]);
                    if (strcmp(args[0], ask_sum) == 0 && n_arg == 3) // ASK_SUM data tipo
                    {
                        /* --------------------------------- ASK_SUM -------------------------------- */
                        int day, mon, year;
                        int sum = 0;
                        char registerName[NAME_REG_LEN];

                        printf("Mi hanno chiesto il totale dei %s del %s\n", args[2], args[1]);

                        // controllo sul tipo
                        if (!validateType(args[2]))
                            goto clean;

                        // controllo sulla data
                        if (sscanf(args[1], "%d_%d_%d", &year, &mon, &day) != 3 || !dateValidation(day, mon, year))
                        {
                            printf("Periodo inserito non valido.\n");
                            goto clean;
                        }

                        // recupero la somma che mi chiedono
                        sprintf(registerName, "./Register%s/Register_%s_%s.txt", argv[1], argv[1], args[1]);
                        sum = sumRecord(registerName, args[2]);

                        // invio la risposta
                        sprintf(buffer, "RESPONSE_SUM %d", sum);
                        ret = send(fd_curr, (void *)buffer, BUFFER_LEN, 0);
                        if (ret < 0)
                        {
                            perror("Errore in fase di send");
                            goto clean;
                        }

                        printf("Ho inviato la somma richiesta.\n");
                    }
                    else if (strcmp(args[0], end) == 0)
                    {
                        // il server si e' spento
                        if (next_sd != -1)
                        {
                            close(next_sd);
                            FD_CLR(next_sd, &current_socket);
                        }
                        if (back_sd != -1)
                        {
                            close(back_sd);
                            FD_CLR(back_sd, &current_socket);
                        }

                        close(sd_tcp);
                        close(sd_udp);
                        exit(EXIT_SUCCESS);
                    }
                    else if (strcmp(args[0], back) == 0 && n_arg == 3 && strcmp(args[2], argv[1]) != 0) // BACK 127.0.0.1 port
                    {
                        // pulisco la vecchia connessione
                        if (back_sd != -1)
                        {
                            //pulisco il vecchio vicino
                            close(back_sd);
                            n_neighbor--;
                            FD_CLR(back_sd, &current_socket);
                            back_sd = -1;
                        }

                        printf("Il mio nuovo vicino back e': %s\n", args[2]);
                        n_neighbor++;
                        back_sd = fd_curr;
                        continue;
                    }
                    else if (strcmp(args[0], next) == 0 && n_arg == 3 && strcmp(args[2], argv[1]) != 0) // NEXT 127.0.0.1 port
                    {
                        // pulisco la vecchia connessione
                        if (next_sd != -1)
                        {
                            close(next_sd);
                            n_neighbor--;
                            FD_CLR(next_sd, &current_socket);
                            next_sd = -1;
                        }

                        printf("Il mio nuovo vicino next e': %s\n", args[2]);
                        n_neighbor++;
                        next_sd = fd_curr;
                        continue;
                    }
                    else if (strcmp(args[0], new_next) == 0 && n_arg == 3 && strcmp(args[2], argv[1]) != 0) // NEW_NEXT 127.0.0.1 port
                    {
                        // pulisco il vecchio vicino
                        if (next_sd != -1)
                        {
                            close(next_sd);
                            n_neighbor--;
                            FD_CLR(next_sd, &current_socket);
                            next_sd = -1;
                        }

                        // preparo indirizzo del nuovo vicino
                        next_sd = socket(AF_INET, SOCK_STREAM, 0);
                        memset(&next_neighbor_addr, 0, sizeof(next_neighbor_addr));
                        next_neighbor_addr.sin_family = AF_INET;
                        next_neighbor_addr.sin_port = htons(atoi(args[2]));
                        inet_pton(AF_INET, args[1], &next_neighbor_addr.sin_addr);

                        // mi connetto al nuovo vicino
                        ret = connect(next_sd, (struct sockaddr *)&next_neighbor_addr, sizeof(next_neighbor_addr));
                        if (ret < 0)
                        {
                            perror("Errore in fase di connect");
                            goto clean;
                        }

                        // avverto il nuovo vicino di aggiornarsi
                        sprintf(buffer, "%s %s %s", back, myAddr, argv[1]);
                        ret = send(next_sd, (void *)buffer, BUFFER_LEN, 0);
                        if (ret < 0)
                        {
                            perror("Errore in fase di send");
                            goto clean;
                        }

                        // inserisco il peer nella select
                        FD_SET(next_sd, &current_socket);
                        if (next_sd > fd_max)
                            fd_max = next_sd;
                    }
                    else if (fd_curr == back_sd && strcmp(args[0], flood) == 0 && n_arg == 5) //FLOOD_FOR_ENTRIES data tipo n_peer
                    {
                        int count = 0;
                        char registerName[NAME_REG_LEN];
                        int n_step = atoi(args[3]) - 1;

                        // propago il flood
                        if (n_step > 1)
                        {
                            sprintf(buffer, "%s %s %s %d %s", args[0], args[1], args[2], n_step, args[4]);
                            //propago il messaggio
                            ret = send(next_sd, (void *)buffer, BUFFER_LEN, 0);
                            if (ret < 0)
                            {
                                perror("Errore in fase di send");
                                continue;
                            }

                            printf("Ho propagato il messaggio.\n");
                        }

                        // controllo se ho i dati richiesti
                        sprintf(registerName, "./Register%s/Register_%s_%s.txt", argv[1], argv[1], args[1]);
                        count = countRecord(registerName, args[2]);

                        if (count != 0)
                        {
                            // invio la risposta indietro
                            printf("Ho il dato che chiedono.\n");
                            sprintf(buffer, "%s %s %s", find, argv[1], args[4]);
                            ret = send(back_sd, (void *)buffer, BUFFER_LEN, 0);
                            if (ret < 0)
                            {
                                perror("Errore in fase di send");
                                continue;
                            }
                        }
                        continue;
                    }
                    else if (fd_curr == next_sd && strcmp(args[0], find) == 0 && n_arg == 3) // FIND porta id
                    {

                        ret = send(back_sd, (void *)buffer, BUFFER_LEN, 0);
                        if (ret < 0)
                        {
                            perror("Errore in fase di send");
                            continue;
                        }

                        printf("Ho propagato il messaggio.\n");
                        continue;
                    }
                    else if ((fd_curr == next_sd || fd_curr == back_sd) && strcmp(args[0], req_data) == 0 && n_arg == 3) //REQ_DATA tipo data
                    {
                        // un vicino mi sta chiedendo se ho un risultato in cache
                        int day, mon, year;
                        struct resultCache *target = NULL;

                        // verifico il tipo dei dati
                        if (!validateType(args[1]))
                            continue;

                        // verifico la data
                        if (sscanf(args[2], "%i_%i_%i", &year, &mon, &day) != 3 || !dateValidation(day, mon, year))
                        {
                            printf("formata della data non valido\n");
                            continue;
                        }

                        //controllo se posseggo il dato
                        target = searchResult(args[2], args[1]);
                        if (target == NULL)
                        {
                            strcpy(buffer, "REPLAY_DATA");
                            printf("Non ho il dato in cache.\n");
                        }
                        else
                        {
                            sprintf(buffer, "REPLAY_DATA %i", target->sum);
                            printf("Ho il dato in cache.\n");
                        }

                        // invio il risultato trovato
                        ret = send(fd_curr, (void *)buffer, BUFFER_LEN, 0);
                        if (ret < 0)
                        {
                            perror("Errore in fase di send");
                            continue;
                        }

                        continue;
                    }
                    else if (strcmp(args[0], ask) == 0 && n_arg == 3) //ASK date tipo
                    {
                        int day, mon, year, sum;
                        char targetRegister[NAME_REG_LEN];

                        // controllo il tipo
                        if (!validateType(args[2]))
                            goto clean;

                        // controllo la data
                        if (sscanf(args[1], "%i_%i_%i", &year, &mon, &day) != 3 || !dateValidation(day, mon, year))
                        {
                            printf("formata della data non valido\n");
                            goto clean;
                        }

                        //cerco valore
                        sprintf(targetRegister, "./Register%s/Register_%s_%s.txt", argv[1], argv[1], args[1]);
                        sum = countRecord(targetRegister, args[2]);

                        // invio una risposta
                        sprintf(buffer, "REPLAY %d", sum);
                        ret = send(fd_curr, (void *)buffer, BUFFER_LEN, 0);
                        if (ret < 0)
                        {
                            perror("Errore in fase di send");
                            goto clean;
                        }

                        printf("Ho rispostao al server.\n");
                    }
                    else if ((fd_curr == next_sd || fd_curr == back_sd) && strcmp(args[0], transfer) == 0 && n_arg == 4) // TRANSFER data tipo num
                    {
                        int day, mon, year;
                        char nameRegister[NAME_REG_LEN];
                        char record[RECORD_LEN];
                        FILE *fp;

                        // verifico il formato dei dati
                        if (!validateType(args[2]))
                            continue;

                        // verifico la data
                        if (sscanf(args[1], "%i_%i_%i", &year, &mon, &day) != 3 || !dateValidation(day, mon, year))
                        {
                            printf("formata della data non valido\n");
                            continue;
                        }

                        sprintf(nameRegister, "./Register%s/Register_%s_%s.txt", argv[1], argv[1], args[1]);

                        // apro il file in scrittura
                        fp = fopen(nameRegister, "a");
                        if (fp == NULL)
                        {
                            printf("Errore nell' apertura del Register.\n");
                            continue;
                        }

                        // creazione del record
                        sprintf(record, "%s %s %s\n", args[1], args[2], args[3]);

                        // scrittura del record e chiusura del file
                        fprintf(fp, "%s", record);
                        fclose(fp);
                        continue;
                    }
                    else
                        printf("Formato del messaggio non valido.\n");

                    // ripulisco la select
                clean:
                    close(fd_curr);
                    FD_CLR(fd_curr, &current_socket);
                }
            }
        }
    }
}