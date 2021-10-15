/* -------------------------------------------------------------------------- */
/*                                     DS                                     */
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

/* -------------------------------------------------------------------------- */
/*                                  COSTANTI                                  */
/* -------------------------------------------------------------------------- */

#define INPUT_LEN 128
#define ARG_LEN 24
#define BUFFER_LEN 256
#define RECORD_LEN 26
#define POOLING_TIME 10
#define NUM_ARG 4
/* -------------------------------------------------------------------------- */
/*                               STRUTTURE DATI                               */
/* -------------------------------------------------------------------------- */

struct peer
{
    int port;
    char *addr;
    struct peer *next;
    struct peer *back;
};

/* -------------------------------------------------------------------------- */
/*                              VARIABILI GLOBALI                             */
/* -------------------------------------------------------------------------- */

struct peer *tail = NULL;
int n_peer = 0;

// messaggi (vedi peer.c per il significato del messaggio)
const char *ask = "ASK";
const char *quit = "QUIT";
const char *join = "JOIN";

/* -------------------------------------------------------------------------- */
/*                               FUNZIONI LISTE                               */
/* -------------------------------------------------------------------------- */

struct peer *insert(int port, const char addr[ARG_LEN], struct sockaddr_in cl_addr)
{
    struct peer *new_node, *head;

    new_node = malloc(sizeof(struct peer));
    if (new_node == NULL)
    {
        perror("malloc non riuscita.\n");
        exit(EXIT_FAILURE);
    }

    n_peer++;

    new_node->port = port;
    new_node->addr = strdup(addr);

    if (tail == NULL)
    {
        new_node->next = new_node;
        new_node->back = new_node;
    }
    else
    {
        head = tail->next;
        new_node->next = head;
        tail->next = new_node;
        tail = new_node;
        head->back = tail;
        return tail;
    }
    return new_node;
}

struct peer *insertSort(int port, char addr[ARG_LEN], struct sockaddr_in cl_addr)
{
    struct peer *new_node = NULL, *head = NULL, *appo = NULL, *prec = NULL;
    if (tail == NULL)
    {
        tail = insert(port, addr, cl_addr);
        return tail;
    }

    new_node = malloc(sizeof(struct peer));
    n_peer++;

    head = tail->next;
    appo = head;

    do
    {
        prec = appo;
        appo = appo->next;
    } while (appo != head && appo->port < port);

    if (head->port < port)
    {
        new_node->port = port;
        new_node->addr = strdup(addr);
        prec->next = new_node;
        new_node->back = prec;
        new_node->next = appo;
        appo->back = new_node;
        if (appo == head)
            tail = new_node;
        return new_node;
    }
    else
    {
        new_node->port = port;
        new_node->addr = strdup(addr);
        new_node->next = head;
        head->back = new_node;
        tail->next = new_node;
        new_node->back = tail;
        return new_node;
    }
}

struct peer *searchPeer(int port)
{
    struct peer *curr;
    int i;

    if (n_peer == 0)
        return NULL;
    curr = tail->next;

    // se non lo trovo ritorno errore
    for (i = 0; i < n_peer; i++)
    {
        if (curr->port == port)
        {
            return curr;
        }
        else
            curr = curr->next;
    }

    return NULL;
}

int deletePeer(int port)
{
    struct peer *curr = tail->next;
    int i;
    if (n_peer == 0)
        return 0;
    for (i = 0; i < n_peer; i++)
    {
        if (curr->port == port)
        {
            curr->next->back = curr->back;
            curr->back->next = curr->next;
            free(curr);
            n_peer--;
            return 1;
        }
        else
            curr = curr->next;
    }
    return 0;
}

void deleteAll()
{
    int i;
    struct peer *curr = tail->next, *next = NULL;
    for (i = 0; i < n_peer; i++)
    {
        next = curr->next;
        free(curr);
        curr = next;
    }

    tail = NULL;
    n_peer = 0;
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

int dateValidation(int day, int mon, int year)
{
    int isLeap = 0;
    int isValid = 1;

    if (year >= 2018 && year <= 9999)
    {
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))
            isLeap = 1;
        if (mon >= 1 && mon <= 12)
        {
            if (mon == 2)
            {
                if (isLeap && day == 29)
                    isValid = 1;
                else if (day > 28)
                    isValid = 0;
            }
            else if (mon == 4 || mon == 6 || mon == 9 || mon == 11)
            {
                if (day > 30)
                    isValid = 0;
            }
            else if (day > 31)
                isValid = 0;
        }
        else
            isValid = 0;
    }
    else
        isValid = 0;

    return isValid;
}

/* -------------------------------------------------------------------------- */
/*                              SERVIZI DS SERVER                             */
/* -------------------------------------------------------------------------- */

void showpeer()
{

    struct peer *curr;
    int i;

    if (n_peer == 0)
    {
        printf("Non ci sono peer connessi.\n");
        return;
    }
    curr = tail->next;
    printf("I peer connessi sono:\n");
    for (i = 0; i < n_peer; i++)
    {
        printf(" - %d\n", curr->port);
        curr = curr->next;
    }
}

void showneighbor(int port)
{
    int i;

    if (port == -1)
    {
        //mostra tutti
        struct peer *curr = tail->next;
        switch (n_peer)
        {
        case 0:
            printf("Non ci sono peer registrati.\n");
            break;
        case 1:
            printf(" - %d -> non ha vicini.\n", curr->port);
            break;
        case 2:
            printf(" - %d -> %d\n", curr->port, curr->next->port);
            printf(" - %d -> %d\n", curr->next->port, curr->port);
            break;
        default:
            for (i = 0; i < n_peer; i++)
            {
                printf(" - %d -> %d, %d\n", curr->port, curr->next->port, curr->back->port);
                curr = curr->next;
            }
            break;
        }
    }
    else
    {
        //mostra specifico
        struct peer *target = NULL, *next = NULL, *back = NULL;
        target = searchPeer(port);
        // non trovato
        if (target == NULL)
        {
            printf("Il peer %d cercato non e' connesso.\n", port);
            return;
        }

        next = target->next;
        back = target->back;

        //tutti i casi possibili
        if (next->port == port && back->port == port)
        {
            //un solo peer
            printf(" - %d -> non ha vicini.\n", port);
            return;
        }
        if (next->port != port && next->port == back->port)
        {
            // due soli peer
            printf(" - %d -> %d\n", port, next->port);
            return;
        }
        // molti peer
        printf(" - %d -> %d, %d\n", port, next->port, back->port);
    }
    return;
}

void esc()
{
    return;
}

void updateNeighbor(int port, char *msg)
{
    // prendo i dati sui nuovi vicini
    char sPort[ARG_LEN];
    struct peer *back = NULL, *next = NULL;
    struct peer *target = searchPeer(port);

    if (target == NULL)
        return;

    back = target->back;
    next = target->next;

    //preparo il messaggio
    strcpy(msg, "UPDATE ");
    if (n_peer > 1)
    {
        sprintf(sPort, "%d", back->port);
        strcat(msg, back->addr);
        strcat(msg, " ");
        strcat(msg, sPort);
        if (n_peer > 2)
        {
            sprintf(sPort, "%d", next->port);
            strcat(msg, " ");
            strcat(msg, next->addr);
            strcat(msg, " ");
            strcat(msg, sPort);
        }
    }
    return;
}

void userMessage()
{
    //messaggio iniziale
    printf("********************************** DS COVID STARTED ********************************** \n");
    printf("Digita un comando:\n\n");
    printf("1) help --> mostra i dettagli dei comandi\n");
    printf("2) showpeer --> mostra un elenco dei peer connessi\n");
    printf("3) showneighbor <peer> --> mostra i neighbor in un peer\n");
    printf("4) esc --> chiude il DS dettaglio comandi\n\n");
}

/* -------------------------------------------------------------------------- */
/*                                    MAIN                                    */
/* -------------------------------------------------------------------------- */

int main(int argc, char *argv[])
{

    /* ------------------- varibili di inizializzazione socket ------------------ */

    int ret, sd_udp, sd_tcp, new_sd, len, addrlen, port, fd_max;
    struct sockaddr_in my_addr, connecting_addr;
    fd_set current_socket, ready_socket;
    int enable = 1;
    char buffer[BUFFER_LEN];

    /* ----------------------------- socket e select ---------------------------- */
    FD_ZERO(&current_socket);
    FD_ZERO(&ready_socket);

    // creazione del socket
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

    // creazione dell'indirizzo del ds server
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

    // preparo la select per scorrere
    FD_SET(0, &current_socket);
    FD_SET(sd_udp, &current_socket);
    FD_SET(sd_tcp, &current_socket);
    if (sd_tcp > sd_udp)
        fd_max = sd_tcp;
    else
        fd_max = sd_udp;

    // messaggio iniziale
    userMessage();

    /* --------------------------------- demone --------------------------------- */

    while (1)
    {
        //preparazione fd_set
        int fd_curr;
        ready_socket = current_socket;
        select(fd_max + 1, &ready_socket, NULL, NULL, NULL);
        for (fd_curr = 0; fd_curr <= fd_max + 1; fd_curr++)
        {
            if (FD_ISSET(fd_curr, &ready_socket))
            {
                if (fd_curr == 0)
                {

                    /* -------------------------------------------------------------------------- */
                    /*                                    STDIN                                   */
                    /* -------------------------------------------------------------------------- */
                    char input[INPUT_LEN];
                    char cmd[ARG_LEN];
                    int port = -1;

                    // leggo comando tastiera
                    fgets(input, sizeof(input), stdin);
                    sscanf(input, "%s %d", cmd, &port);

                    // decide quale comando eseguire
                    if (strcmp(cmd, "help") == 0 && port == -1)
                    {
                        printf("************************************* HELP *************************************\n");
                        printf("SHOWPEER:\n Il seguente comando mostra l'elenco di tutti i peer connessi alla rete, identificandoli con il loro numero di porta.\n Se non ci sono peer connessi viene restituita una lista vuota.\n\n");
                        printf("SHOWNEIGHBOR <PEER>:\n Il seguente comando mostra i neighbor di un peer selezionato attraverso il numero di porta.\n Se la porta non viene selezionata vengono mostrati i neighbor di tutti i peer.\n\n");
                        printf("ESC:\n Il seguente comando termina l'esecuzione del DS.\n In seguito a quaesto comando termina anche l'esecuzione di tutti i peer connessi.\n\n");
                    }
                    else if (strcmp(cmd, "showpeer") == 0 && port == -1)
                    {
                        printf("*********************************** SHOWPEER ***********************************\n");
                        showpeer();
                    }
                    else if (strcmp(cmd, "showneighbor") == 0)
                    {
                        printf("********************************* SHOWNEIGHBOR *********************************\n");
                        showneighbor(port);
                    }
                    else if (strcmp(cmd, "esc") == 0 && port == -1)
                    {
                        printf("************************************* ESC **************************************\n");
                        // invio a tutti i peer l'ordine di terminare
                        int i;
                        struct peer *target, *curr = tail->next;
                        for (i = 0; i < n_peer; i++)
                        {
                            struct sockaddr_in peer_addr;
                            int peer_sd, flag_missing;

                            // indirizzo del peer
                            peer_sd = socket(AF_INET, SOCK_STREAM, 0);
                            memset(&peer_addr, 0, sizeof(peer_addr));
                            peer_addr.sin_family = AF_INET;
                            peer_addr.sin_port = htons(curr->port);
                            inet_pton(AF_INET, curr->addr, &peer_addr.sin_addr);

                            // mi connetto al peer
                            ret = connect(peer_sd, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                            if (ret < 0)
                            {
                                perror("Errore in fase di connect");
                                close(peer_sd);
                                continue;
                            }

                            // invio il messaggio al peer
                            strcpy(buffer, "END");
                            ret = send(peer_sd, (void *)buffer, BUFFER_LEN, 0);
                            if (ret < 0)
                            {
                                perror("Errore in fase di send");
                                close(peer_sd);
                                continue;
                            }

                            // scorro i peer e chiudo il socket
                            curr = curr->next;
                            close(peer_sd);
                        }

                        //chiudo i socket
                        close(sd_tcp);
                        close(sd_udp);
                        exit(EXIT_SUCCESS);
                    }
                    else
                    {
                        printf("Comando non valido. \n\n");
                    }
                    userMessage();
                }
                else if (fd_curr == sd_udp)
                {
                    /* -------------------------------------------------------------------------- */
                    /*                               SD_UDP LISTENER                              */
                    /* -------------------------------------------------------------------------- */

                    int n_arg = NUM_ARG;
                    char args[NUM_ARG][ARG_LEN];
                    struct peer *target = NULL;

                    // lunghezza indirizzi
                    addrlen = sizeof(connecting_addr);

                    // ricevo messaggi dai peer
                    do
                    {
                        ret = recvfrom(sd_udp, buffer, BUFFER_LEN, 0, (struct sockaddr *)&connecting_addr, &addrlen);
                        if (ret < 0)
                            sleep(POOLING_TIME);
                    } while (ret < 0);
                    printf("Ho ricevuto un messaggio UDP.\n");

                    // verifico il messaggio ricevuto
                    n_arg = sscanf(buffer, "%s %s %s %s", args[0], args[1], args[2], args[3]);
                    if (strcmp(args[0], join) == 0 && n_arg == 3) // JOIN 127.0.0.1 port
                    {
                        printf("Ho ricevuto una richiesta di registarzaione.\n");
                        // controllo se il peer e' gia' inserito
                        target = searchPeer(atoi(args[2]));

                        // inserisco il vicino
                        if (target == NULL)
                            target = insertSort(atoi(args[2]), args[1], connecting_addr);
                        else
                        {
                            deletePeer(target->port);
                            target = insertSort(atoi(args[2]), args[1], connecting_addr);
                        }

                        // invio la conferma di registrazione al peer inviando i vicini
                        updateNeighbor(target->port, buffer);
                        do
                        {
                            ret = sendto(sd_udp, buffer, BUFFER_LEN, 0, (struct sockaddr *)&connecting_addr, sizeof(connecting_addr));
                            if (ret < 0)
                                sleep(POOLING_TIME);
                        } while (ret < 0);

                        // attesa per evitare inconsistenze in caso di join multiple
                        printf("Ho inviato i vicini al nuovo peer.\n");
                        sleep(4);
                    }
                    else
                        printf("Formato UDP non valido.\n");
                }
                else if (fd_curr == sd_tcp)
                {
                    /* -------------------------------------------------------------------------- */
                    /*                                     TCP                                    */
                    /* -------------------------------------------------------------------------- */
                    addrlen = sizeof(connecting_addr);

                    // accetto la richiesta del peer
                    new_sd = accept(sd_tcp, (struct sockaddr *)&connecting_addr, &addrlen);

                    // inserisco nel set dei socket
                    FD_SET(new_sd, &current_socket);
                    if (new_sd > fd_max)
                        fd_max = new_sd;
                }
                else
                {
                    /* -------------------------------------------------------------------------- */
                    /*                              LAVORO CON I PEER                             */
                    /* -------------------------------------------------------------------------- */
                    // variabili per il controllo dei messaggi
                    int n_arg;
                    char args[NUM_ARG][ARG_LEN];
                    struct peer *target = NULL;

                    // ricevo il messaggio
                    ret = recv(fd_curr, (void *)buffer, BUFFER_LEN, 0);
                    if (ret < 0)
                    {
                        perror("Errore in fase di recv");
                        goto clean;
                    }
                    printf("Ho ricevuto un messaggio TCP.\n");

                    // controllo tipo di richiesta gli argomenti
                    n_arg = sscanf(buffer, "%s %s %s %s", args[0], args[1], args[2], args[3]);
                    if (strcmp(args[0], ask) == 0 && n_arg == 4) // ASK port curr_date tipo
                    {
                        struct peer *target, *curr;
                        int day, mon, year, i;
                        int missingRecord = 0;
                        int num = 0;

                        // verifico la data richiesta
                        if (sscanf(args[2], "%i_%i_%i", &year, &mon, &day) != 3 || !dateValidation(day, mon, year))
                        {
                            printf("Formata della data non valido\n");
                            goto clean;
                        }

                        // verifico il tipo richiesto
                        if (!validateType(args[3]))
                        {
                            printf("Tipo inserito non valido.\n");
                            goto clean;
                        }

                        // cerco il peer richiedente
                        target = searchPeer(atoi(args[1]));
                        if (target == NULL)
                        {
                            printf("Il peer richiedente non e' registrato");
                            goto clean;
                        }

                        printf("Mi hanno chiesto informazioni sul giorno %s\n", args[2]);

                        // scorro i peer registrati
                        curr = tail->next;
                        for (i = 0; i < n_peer; i++)
                        {
                            struct sockaddr_in peer_addr;
                            int peer_sd, flag_missing;

                            // non chiedo al richiedente
                            if (curr->port == target->port)
                            {
                                curr = curr->next;
                                continue;
                            }

                            // indirizzo del peer
                            peer_sd = socket(AF_INET, SOCK_STREAM, 0);
                            memset(&peer_addr, 0, sizeof(peer_addr));
                            peer_addr.sin_family = AF_INET;
                            peer_addr.sin_port = htons(curr->port);
                            inet_pton(AF_INET, curr->addr, &peer_addr.sin_addr);

                            // mi connetto al peer
                            ret = connect(peer_sd, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                            if (ret < 0)
                            {
                                perror("Errore in fase di connect");
                                close(peer_sd);
                                continue;
                            }

                            // invio il messaggio al peer
                            sprintf(buffer, "%s %s %s", ask, args[2], args[3]);
                            ret = send(peer_sd, (void *)buffer, BUFFER_LEN, 0);
                            if (ret < 0)
                            {
                                perror("Errore in fase di send");
                                close(peer_sd);
                                continue;
                            }

                            //attendo la risposta
                            ret = recv(peer_sd, (void *)buffer, BUFFER_LEN, 0);
                            if (ret < 0)
                            {
                                perror("Errore in fase di recv");
                                close(peer_sd);
                                continue;
                            }

                            // verifico la riposta ricevuta
                            if (sscanf(buffer, "REPLAY %d", &flag_missing) != 1)
                            {
                                printf("Messaggio non valido.\n");
                                close(peer_sd);
                                continue;
                            }

                            // non tutti i record sono in possesso del richiedente
                            if (flag_missing != 0)
                                missingRecord++;

                            // scorro i peer e chiudo il socket
                            curr = curr->next;
                            close(peer_sd);
                        }

                        // confronto il numero di record
                        if (missingRecord == 0)
                        {
                            strcpy(buffer, "OK");
                            printf("Il peer ha tutte le informazioni.\n");
                        }
                        else
                        {
                            sprintf(buffer, "MISSING %d %d", missingRecord, n_peer);
                            printf("Il peer non ha tutte le informazioni.\n");
                        }

                        // invio la risposta al peer richiedente
                        ret = send(fd_curr, (void *)buffer, BUFFER_LEN, 0);
                        if (ret < 0)
                        {
                            perror("Errore in fase di send");
                            goto clean;
                        }
                    }
                    else if (strcmp(args[0], quit) == 0 && n_arg == 2) // QUIT port
                    {
                        // variabili di appoggio
                        struct peer *target;
                        struct sockaddr_in peer_addr;
                        int peer_sd;

                        target = searchPeer(atoi(args[1]));
                        printf("Ricevuto richiesta di disconnessione.\n");

                        if (target != NULL && n_peer > 1)
                        {
                            //avverto il vicino del nuovo vicinato
                            peer_sd = socket(AF_INET, SOCK_STREAM, 0);
                            memset(&peer_addr, 0, sizeof(peer_addr));
                            peer_addr.sin_family = AF_INET;
                            peer_addr.sin_port = htons(target->back->port);
                            inet_pton(AF_INET, target->back->addr, &peer_addr.sin_addr);

                            // mi connetto al peer richiedenete
                            ret = connect(peer_sd, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
                            if (ret < 0)
                            {
                                perror("Errore in fase di connect");
                                close(peer_sd);
                                goto clean;
                            }

                            sprintf(buffer, "NEW_NEXT %s %d", target->next->addr, target->next->port);
                            ret = send(peer_sd, (void *)buffer, BUFFER_LEN, 0);
                            if (ret < 0)
                            {
                                perror("Errore in fase di send");
                                close(peer_sd);
                                goto clean;
                            }

                            // chiudo il socket ed elimino il peer
                            close(peer_sd);
                            deletePeer(atoi(args[1]));
                            printf("Ho aggiornato il vicinato.\n");
                        }
                        else
                            printf("Non ci sono peer connessi.\n");
                    }
                    else
                        printf("Richiesta non valida\n");

                    //pulisco la connessione
                clean:
                    close(fd_curr);
                    FD_CLR(fd_curr, &current_socket);
                }
            }
        }
    }
}
