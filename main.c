#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "pthread.h"
#include <signal.h>
#include "MQTTPacket.h"
#include "defs.h"

#define PORT 1883
//#define IP_LOCAL "3.120.25.91" //"127.0.0.1"
#define IP_REMOTO "3.120.25.91"
#define CONNECTED 1
#define DISCONNECTED 0
#define TX_ENABLE 1
#define TX_DISABLE 0
#define BUFFER_SIZE 128
#define BACKLOG 10

static char IO_file_name[] = "/home/fedepacher/CESE/MQTT Server/MQTT-Server/IO_file.xml";

typedef struct
{
	char topic[20];
	unsigned char data[BUFFER_SIZE];
	uint32_t length;
} dataMqtt_t;

pthread_t thread_publish;	//FD para thread socket
pthread_t thread_subscribe; //FD para thread edu ciaa

int fd_socket;

int flag_sign;
uint8_t flag_connected;
uint8_t flag_transmition_file;

//static char topic_pub[] = "topic/pub\0";
//static char topic_sub[] = "topic/sub\0";
//static char topic_request_file[] = "topic/request\0";

static char subscribe_topics[][BUFFER_SIZE] =
	{
		"ME/ID0001RX/PC/Permiso\0",
		"ME/ID0001TX/PC\0",
};

static char publish_topics[][BUFFER_SIZE] =
	{
		"ME/ID0001RX/PC\0",
};

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; //mutex para resguardar el flag de estado de conexión

/**
 * @brief Thread Publish Handler 
 * @param argParam		
 * @return void
 */
void *PublishHandler(void *argParam); //handler del Publish

/**
 * @brief Thread Subscribe Handler 
 * @param argParam		
 * @return void
 */
void *SubscribeHandler(void *argParam); //handler del Subscribe

static void event_Sign(char *msg, uint32_t length); //Handler general de SIGTERM Y SIGINT

void bloquearSign(void);	//Bloque de señales
void desbloquearSign(void); //Desbloque de señales

static void SetFlag(uint8_t *flag, int state); //seteo de flag de conexion/desconexion

static int ReadFlagState(uint8_t *flag); //leo el estado de flag_connected protegido con mutex

/**
 * @brief Find a substring in a string 
 * @param string	data in string 
 * @param length	string param length 
 * @param toFind	string to find
 * @return 1 if it is found, 0 if not, -1 if toFind > string
 */
static uint8_t strContains(char *string, uint32_t length, char *toFind);

/**
 * @brief Publish msj to mqtt broker 
 * @param buff		store connect data frame 
 * @param length	buffer length
 * @return data frame length
 */
uint32_t connectMqttBroker(uint8_t *buff, uint32_t length); //conexion con broker mqtt

/**
 * @brief Publish msj to mqtt broker 
 * @param buff_in	data to publish
 * @param length_in	data length to publish
 * @param buff_out	data frame to publish
 * @param length	data frame length 
 * @return length to publish
 */
uint32_t Publish(dataMqtt_t *buff_in, uint32_t length_in, uint8_t *buff_out, uint32_t length);

/**
 * @brief Create socket and connet to mqtt server
 * @return 1 if it is ok, otherwise -1
 */
int socketConnect(void);

/**
 * @brief Create File
 * @param buffer	buffer to store in file
 * @param length	length of buffer
 * @return void
 */
void CreateFile(uint8_t *buffer, uint32_t length); //creacion de archivo

static void event_Sign(char *msg, uint32_t length)
{

	write(1, msg, length);

	//Cerramos el socket
	close(fd_socket);

	//Detenemos threads
	pthread_cancel(thread_publish);
	pthread_cancel(thread_subscribe);
}

//Handler SIGINT
void event_SIGINT(int sig)
{
	flag_sign = SIGINT;
}

//Handler de SIGTERM
void event_SIGTERM(int sig)
{
	flag_sign = SIGTERM;
}

void bloquearSign(void)
{
	sigset_t set;
	sigemptyset(&set);
	sigfillset(&set);
	pthread_sigmask(SIG_BLOCK, &set, NULL);
}

void desbloquearSign(void)
{
	sigset_t set;
	sigemptyset(&set);
	sigaddset(&set, SIGINT);
	sigaddset(&set, SIGTERM);
	pthread_sigmask(SIG_UNBLOCK, &set, NULL);
}

static void SetFlag(uint8_t *flag, int state)
{
	pthread_mutex_lock(&mutex);
	flag_connected = state;
	pthread_mutex_unlock(&mutex);
}

static int ReadFlagState(uint8_t *flag)
{
	uint8_t flag_state;
	pthread_mutex_lock(&mutex);
	flag_state = *flag;
	pthread_mutex_unlock(&mutex);

	return flag_state;
}

int main(void)
{
	int retVal_t1 = 0;
	int retVal_t2 = 0;
	struct sigaction sa1;
	struct sigaction sa2;
	char msg[50];
	flag_sign = 0;

	SetFlag(&flag_connected, DISCONNECTED);		 //Seteamos flag a desconectado
	SetFlag(&flag_transmition_file, TX_DISABLE); //seteamos flag a disable

	//Inicializamos estructura para evento SIGINT
	sa1.sa_handler = event_SIGINT;
	sa1.sa_flags = SA_RESTART; //
	sigemptyset(&sa1.sa_mask);

	//Inicializamos estructura para ev ento SIGTERM
	sa2.sa_handler = event_SIGTERM;
	sa2.sa_flags = SA_RESTART; //
	sigemptyset(&sa2.sa_mask);

	printf("Conectando a broker MQTT...\r\n");

	if (socketConnect() == 1) //conectamos a socket y posteriormente a broker
	{
		//Bloqueo de señales
		bloquearSign();

		//Creacion de threads
		if (ReadFlagState(&flag_connected) == CONNECTED)
		{
			retVal_t1 = pthread_create(&thread_publish, NULL, PublishHandler, NULL);	 //thread para publicar
			retVal_t2 = pthread_create(&thread_subscribe, NULL, SubscribeHandler, NULL); //thread para subscribir
		}

		//Desbloqueo de señales
		desbloquearSign();

		//Chequeo de errores de creacion de thread
		if (retVal_t1 || retVal_t2)
		{
			perror("Error creacion de thread");
			return -1;
		}

		//Activamos las señales
		if (sigaction(SIGINT, &sa1, NULL) == -1)
		{
			perror("Error sigaction");
			exit(1);
		}

		if (sigaction(SIGTERM, &sa2, NULL) == -1)
		{
			perror("Error sigaction");
			exit(1);
		}

		while (flag_sign == 0)
		{
			sleep(1);
		}

		//DESCONECTARSE DEL BROKER POR RESPETO, SORETE

		switch (flag_sign)
		{
		case SIGINT:
			sprintf(msg, "Llego SIGINT\r\n");
			break;
		case SIGTERM:
			sprintf(msg, "Llego SIGTERM\r\n");
			break;
		}
	}
	event_Sign(msg, strlen(msg));

	//Liberamos memoria
	pthread_join(thread_publish, NULL);
	pthread_join(thread_subscribe, NULL);

	exit(EXIT_SUCCESS);
	return 0;
}

int socketConnect(void)
{
	struct sockaddr_in server;
	uint8_t buffer[BUFFER_SIZE];
	uint32_t con_length;
	u_int8_t error_flag = 0;
	//Create socket
	fd_socket = socket(AF_INET, SOCK_STREAM, 0);

	if (fd_socket == -1)
	{
		puts("ERROR en socket\r\n");
		return -1;
		//error_flag++;
	}

	server.sin_addr.s_addr = inet_addr(IP_REMOTO); //store ip
	server.sin_family = AF_INET;
	server.sin_port = htons(PORT);

	//Connect to remote server
	if (connect(fd_socket, (struct sockaddr *)&server, sizeof(server)) < 0)
	{
		puts("Error en conexion con broker\r\n");
		//error_flag++;
		return -1;
	}
	if (error_flag == 0)
	{
		puts("Solicitando conexion a broker\r\n");
		con_length = connectMqttBroker(buffer, BUFFER_SIZE); //get data frame to connect to the broker

		//Send some data
		if (send(fd_socket, buffer, con_length, 0) < 0)
		{
			puts("Solicitud fallida\r\n");
			//error_flag++;
			return -1;
		}
		puts("Conectado a broker\r\n");

		if (error_flag == 0)
		{
			SetFlag(&flag_connected, CONNECTED); //set flat to connected
		}
	}
	return 1;
}

void *SubscribeHandler(void *argParam)
{
	int i, j, k;
	int length;
	uint8_t buffer[BUFFER_SIZE];
	uint8_t buffer_aux[BUFFER_SIZE];
	uint8_t flag_start = 0;

	// Populate the subscribe message.
	MQTTString topicFilters[1] = {MQTTString_initializer};
	int requestedQoSs[1] = {0};
	int len = sizeof(subscribe_topics) / sizeof(subscribe_topics[0]);
	for (k = 0; k < len; k++)
	{
		topicFilters[0].cstring = (char *)subscribe_topics[k];
		memset((char *)buffer, '\0', BUFFER_SIZE);
		length = MQTTSerialize_subscribe(buffer, BUFFER_SIZE, 0, 1, 1,
										 topicFilters, requestedQoSs);
		if (send(fd_socket, buffer, length, 0) < 0)
		{
			puts("Receive subscribe failed");
			return NULL;
		}
	}

	memset((char *)buffer, '\0', BUFFER_SIZE);
	while (ReadFlagState(&flag_connected) == CONNECTED)
	{
		memset((char *)buffer, '\0', BUFFER_SIZE);
		memset((char *)buffer_aux, '\0', BUFFER_SIZE);
		if ((length = recv(fd_socket, buffer, BUFFER_SIZE, 0)) > 0)
		{

			//chequeamos que este el pedido de envio de into
			if (ReadFlagState(&flag_transmition_file) == TX_DISABLE && strContains((char *)buffer, length, subscribe_topics[0]) == 1)
			{
				SetFlag(&flag_transmition_file, TX_ENABLE);
				printf("Pedido de Transmision\r\n");
			}
			else
			{
				if (strContains((char *)buffer, length, subscribe_topics[1]) == 1)
				{
					flag_start = 0;
					j = 0;
					for (i = 0; i < length; i++)
					{
						printf("%c", buffer[i]);
						if (buffer[i] == SCH)
						{					//find the char '$'
							flag_start = 1; //set flag if it found
						}
						if (flag_start == 1)
						{ //if flag is set store the content to save it in file
							buffer_aux[j++] = buffer[i];
						}
					}
					if (flag_start == 1)
					{
						buffer_aux[j++] = '\r';
						buffer_aux[j++] = '\n';
						CreateFile(buffer_aux, (j - 1)); //save line in file
					}
					printf("%c%c", '\r', '\n');
				}
				else
				{
					printf("Frame descartado\r\n");
				}
			}
		}
		else
		{
			switch (length)
			{
			case -1:
				perror("Error leyendo mensaje en socket");
				break;
			case 0:
				printf("Recibo de EOF");
				break;
			default:
				printf("Error recibido numero %d", length);
				break;
			}
			break;
		}
		//usleep(1000)
	}
	return NULL;
}

static uint8_t strContains(char *string, uint32_t length, char *toFind)
{
	uint8_t slen = length; //strlen(string);
	uint8_t tFlen = strlen(toFind);
	uint8_t found = 0;

	if (slen >= tFlen)
	{
		for (uint8_t s = 0, t = 0; s < slen; s++)
		{
			do
			{

				if (string[s] == toFind[t])
				{
					if (++found == tFlen)
						return 1;
					s++;
					t++;
				}
				else
				{
					s -= found;
					found = 0;
					t = 0;
				}

			} while (found);
		}
		return 0;
	}
	else
		return -1;
}

void CreateFile(uint8_t *buffer, uint32_t length)
{
	FILE *fp1;

	fp1 = fopen(IO_file_name, "a"); //create output file
	if (fp1 != NULL)
	{
		fputs((char *)buffer, fp1); //append data to the file
	}
	else
	{
		printf("Error creando archivo\r\n");
	}
	fclose(fp1); //close file
}

void *PublishHandler(void *argParam)
{
	FILE *file = NULL;
	uint8_t buffer[BUFFER_SIZE];
	uint32_t pub_length;
	dataMqtt_t data_pub;
	
	strncpy((char *)data_pub.topic, publish_topics[0], strlen((char *)publish_topics[0]));
	while (ReadFlagState(&flag_connected) == CONNECTED)
	{
		//memset((char *)data_pub.data, '\0', BUFFER_SIZE);
		//sprintf((char *)data_pub.data, "contador:%d", contador);
		//contador++;

		if (ReadFlagState(&flag_transmition_file) == TX_ENABLE)
		{
			file = fopen(IO_file_name, "r");						//abro archivo a transmitir
			if (file != NULL)
			{
				while (fgets((char *)buffer, BUFFER_SIZE, file))	//mientras tenga info el archivo me quedo aca adentro
				{
					memset((char *)data_pub.data, '\0', BUFFER_SIZE);
					sprintf((char *)data_pub.data, (char *)buffer);
					pub_length = Publish(&data_pub, strlen((char *)data_pub.data), buffer, BUFFER_SIZE);
					if (send(fd_socket, buffer, pub_length, 0) < 0)
					{
						puts("Error en publish");
						return NULL;
					}
					usleep(1000);
				}
			}
			else
			{
				printf("Archivo no existente\r\n");
			}
			SetFlag(&flag_transmition_file, TX_DISABLE);
		}
		usleep(1000);
	}
	SetFlag(&flag_connected, DISCONNECTED);
	return NULL;
}

uint32_t Publish(dataMqtt_t *buff_in, uint32_t length_in, uint8_t *buff_out, uint32_t length)
{

	int32_t length_out;

	// Populate the publish message.
	MQTTString topicString = MQTTString_initializer;
	topicString.cstring = (char *)buff_in->topic;
	int qos = 0;
	memset((char *)buff_out, '\0', length);
	//strcat((char*)data->data, "\r\n");// OJO QUE PUEDE QUE ALGUNOS ENVIOS NECESITEN ESTE \R\N
	length_out = MQTTSerialize_publish(buff_out, length, 0, qos, 0, 0,
									   topicString, buff_in->data, length_in);

	return length_out;
}

uint32_t connectMqttBroker(uint8_t *buff, uint32_t length)
{
	MQTTPacket_connectData connectData = MQTTPacket_connectData_initializer;
	int32_t length_data_rec;

	connectData.MQTTVersion = 3; //4
	connectData.clientID.cstring = "fede";
	connectData.keepAliveInterval = 120;
	//connectData.willFlag = 1;
	//connectData.will.qos = 2;
	memset((char *)buff, '\0', length);
	length_data_rec = MQTTSerialize_connect(buff, length, &connectData);

	return length_data_rec;
}
