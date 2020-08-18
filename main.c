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


#define	PORT			1883
#define	IP_LOCAL		"35.156.66.227"//"127.0.0.1"
#define	IP_REMOTO		"35.156.66.227"
#define CONNECTED		1
#define DISCONNECTED	0
#define BUFFER_SIZE		128
#define BACKLOG			10

typedef struct{
	char topic[20];
	unsigned char data[BUFFER_SIZE];
	uint32_t length;
}dataMqtt_t;

pthread_t thread_publish;			//FD para thread socket
pthread_t thread_subscribe;				//FD para thread edu ciaa

int fd_socket;

int flag_sign;
uint8_t flag_connected;

static char topic_pub[] = "topic/pub\0";
static char topic_sub[] = "topic/sub\0";


pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;	//mutex para resguardar el flag de estado de conexión


/**
 * @brief Thread Publish Handler 
 * @param argParam		
 * @return void
 */ 
void* PublishHandler(void* argParam);		//handler del Publish 

/**
 * @brief Thread Subscribe Handler 
 * @param argParam		
 * @return void
 */ 
void* SubscribeHandler(void* argParam);		//handler del Subscribe

static void event_Sign(char * msg, uint32_t length);			//Handler general de SIGTERM Y SIGINT

void bloquearSign(void);					//Bloque de señales
void desbloquearSign(void);					//Desbloque de señales


static void SetFlag(uint8_t *flag, int state);	//seteo de flag de conexion/desconexion

static int ReadFlagState(uint8_t *flag);					//leo el estado de flag_connected protegido con mutex	

/**
 * @brief Publish msj to mqtt broker 
 * @param buff		store connect data frame 
 * @param length	buffer length
 * @return data frame length
 */ 
uint32_t connectMqttBroker(uint8_t *buff, uint32_t length);		//conexion con broker mqtt	

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
 * @return void
 */ 
void socketConnect(void);

/**
 * @brief Create File
 * @param buffer	buffer to store in file
 * @param length	length of buffer
 * @return void
 */ 
void CreateFile(uint8_t * buffer, uint32_t length); 		//creacion de archivo


static void event_Sign(char * msg, uint32_t length){
	
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



void bloquearSign(void){
	sigset_t set;
	sigemptyset(&set);
	sigfillset(&set);
	pthread_sigmask(SIG_BLOCK, &set, NULL);
}

void desbloquearSign(void){
	sigset_t set;
	sigemptyset(&set);
	sigaddset(&set, SIGINT);
	sigaddset(&set, SIGTERM);
	pthread_sigmask(SIG_UNBLOCK, &set, NULL);
}

static void SetFlag(uint8_t *flag, int state){
	pthread_mutex_lock (&mutex);
	flag_connected = state;
	pthread_mutex_unlock (&mutex);
}

static int ReadFlagState(uint8_t *flag){
	uint8_t flag_state;
	pthread_mutex_lock (&mutex);
	flag_state = *flag;
	pthread_mutex_unlock (&mutex);

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

	SetFlag(&flag_connected, DISCONNECTED);

	//Inicializamos estructura para evento SIGINT
	sa1.sa_handler = event_SIGINT;
	sa1.sa_flags =  SA_RESTART; //
	sigemptyset(&sa1.sa_mask);

	//Inicializamos estructura para ev ento SIGTERM
	sa2.sa_handler = event_SIGTERM;
	sa2.sa_flags =  SA_RESTART; //
	sigemptyset(&sa2.sa_mask);

	printf("Conectando a broker MQTT...\r\n");
		
	socketConnect();	
	//Bloqueo de señales
	bloquearSign();

	//Creacion de threads 
	if(ReadFlagState(&flag_connected) == CONNECTED){
		retVal_t1 = pthread_create(&thread_publish, NULL, PublishHandler, NULL);
		retVal_t2 = pthread_create(&thread_subscribe, NULL, SubscribeHandler, NULL);
	}

	//Desbloqueo de señales	
	desbloquearSign();

	//Chequeo de errores de creacion de thread
	if(retVal_t1 || retVal_t2){
		perror("Error creacion de thread");
		return -1;
	}

	//Activamos las señales
	if(sigaction(SIGINT, &sa1, NULL) == -1){
		perror("Error sigaction");
		exit(1);
	}

	if(sigaction(SIGTERM, &sa2, NULL) == -1){
		perror("Error sigaction");
		exit(1);
	}

	
	while(flag_sign == 0){
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

	event_Sign(msg, strlen(msg));

	//Liberamos memoria
	pthread_join(thread_publish, NULL);
	pthread_join(thread_subscribe, NULL);

	exit(EXIT_SUCCESS);
	return 0;
}


void socketConnect(void){
	struct sockaddr_in server;
	uint8_t buffer[BUFFER_SIZE];
	uint32_t con_length;
	u_int8_t error_flag = 0;
	//Creamos socket
	fd_socket = socket(AF_INET,SOCK_STREAM, 0);		

	if(fd_socket  == -1){
		puts("ERROR en socket\r\n");
		error_flag++;
	}	

	server.sin_addr.s_addr = inet_addr(IP_LOCAL);
	server.sin_family = AF_INET;
	server.sin_port = htons( PORT );

	//Connect to remote server
	if (connect(fd_socket , (struct sockaddr *)&server , sizeof(server)) < 0)
	{
		puts("connect error");
		error_flag++;
	}	
	puts("Connected");
	con_length = connectMqttBroker(buffer, BUFFER_SIZE);

	//Send some data	
	if( send(fd_socket , buffer , con_length , 0) < 0)
	{
		puts("Send failed");
		error_flag++;
	}
	puts("Data Connect Sent\n");

	if(error_flag == 0){
		SetFlag(&flag_connected, CONNECTED);
	}
}

void* SubscribeHandler(void* argParam){
	int i, j;
	int length;
	uint8_t buffer[BUFFER_SIZE];
	uint8_t buffer_aux[BUFFER_SIZE];
	uint8_t flag_start = 0;
	 
	// Populate the subscribe message.
	MQTTString topicFilters[1] = { MQTTString_initializer };
	topicFilters[0].cstring = topic_sub;//"test/rgb";
	int requestedQoSs[1] = { 0 };
	length = MQTTSerialize_subscribe(buffer, BUFFER_SIZE, 0, 1, 1,
			topicFilters, requestedQoSs);
	if( send(fd_socket , buffer, length , 0) < 0)
	{
		puts("Receive subscribe failed");
		return NULL;
	}	
	memset((char*) buffer, '\0', BUFFER_SIZE);		
	while(ReadFlagState(&flag_connected) == CONNECTED){
		memset((char*) buffer, '\0', BUFFER_SIZE);		
		memset((char*) buffer_aux, '\0', BUFFER_SIZE);		
		length = recv(fd_socket, buffer, BUFFER_SIZE, 0);
		
    	if(length > 0)
    	{
			flag_start = 0;
			j = 0;
			for(i = 0; i < length; i++){
       			printf("%c",buffer[i]);
				if(buffer[i] == SCH){
					flag_start = 1;
				}
				if(flag_start == 1){  
					buffer_aux[j++] = buffer[i];
				}   
			}
			if(flag_start == 1){
				buffer_aux[j++] = '\r';
				buffer_aux[j++] = '\n';	
				CreateFile(buffer_aux, (j - 1));
			}		
			printf("%c%c", '\r', '\n');
    	}
		else
		{
			printf("Error en Subscribe\r\n");
			
		}
		
		
		//usleep(1000)
	}
	return NULL;		
}


void CreateFile(uint8_t * buffer, uint32_t length){
	FILE *fp1;
    char output_file_name[] = "/home/fedepacher/CESE/MQTT Server/MQTT-Server/incoming_file_from_pc.xml";
	
	fp1 = fopen(output_file_name, "a");     //create output file
    if(fp1 != NULL)
    {           
        fputs((char*)buffer, fp1);             //append data to the file   
    }
    else{
        printf("Error creando archivo\r\n");
    }
    fclose(fp1);                            //close file
}

void* PublishHandler(void* argParam){
	uint8_t buffer[BUFFER_SIZE];
	uint32_t pub_length; 
	dataMqtt_t data_pub;
	int contador = 0;
	strncpy((char*)data_pub.topic, topic_pub, strlen((char*)topic_pub));
	while(ReadFlagState(&flag_connected) == CONNECTED){
		memset((char*) data_pub.data, '\0', BUFFER_SIZE);
		sprintf((char*) data_pub.data, "contador:%d", contador);
		contador++;
		
		pub_length = Publish(&data_pub, strlen((char*)data_pub.data), buffer, BUFFER_SIZE);

		//Publish some data	
		if( send(fd_socket , buffer, pub_length , 0) < 0)
		{
			puts("Send publish failed");
			return NULL;
		}	
		sleep(1);
	}
	SetFlag(&flag_connected, DISCONNECTED);
	return NULL;

}

uint32_t Publish(dataMqtt_t *buff_in, uint32_t length_in, uint8_t *buff_out, uint32_t length){

	int32_t length_out;
	
	// Populate the publish message.
	MQTTString topicString = MQTTString_initializer;
	topicString.cstring = (char*)buff_in->topic;
	int qos = 0;
	memset((char*)buff_out, '\0', length);
	//strcat((char*)data->data, "\r\n");// OJO QUE PUEDE QUE ALGUNOS ENVIOS NECESITEN ESTE \R\N
	length_out = MQTTSerialize_publish(buff_out, length, 0, qos, 0, 0,
			topicString, buff_in->data, length_in);


	return length_out;
}


uint32_t connectMqttBroker(uint8_t *buff, uint32_t length){
	MQTTPacket_connectData connectData = MQTTPacket_connectData_initializer;
	int32_t length_data_rec;	

	connectData.MQTTVersion = 3; //4
	connectData.clientID.cstring = "fede";
	connectData.keepAliveInterval = 120;
	//connectData.willFlag = 1;
	//connectData.will.qos = 2;
	memset((char*)buff, '\0', length);
	length_data_rec = MQTTSerialize_connect(buff, length, &connectData);

	return length_data_rec;
}


void* ciaaHandler(void* argParam){
	int32_t data_receive_length = 0;

	
	while (1)
	{
		//Leemos datos del puerto serie
		//data_receive_length = serial_receive(buffer_rx_ciaa, BUFFER_SIZE);
	
		if(data_receive_length > 10){		//Aca puse > 10 porque si ponia 0 estaba constantemente entrando al if
			data_receive_length = 0;
			
			//envio data a la web
			/*if(ReadFlagState(&flag_connected) == CONNECTED){		//agregar el chequeo del disconnect										//chequeo que el fd exista
				if (send (fd_accept, buffer_tx_web, strlen(buffer_tx_web), 0) < 0)
				//if (write (fd_accept, buffer_tx_web, strlen(buffer_tx_web)) == -1)		//envio data a la web
				{
					printf("Error en envio");
					close(fd_accept);
					return NULL;
				}
			}*/						
		}
		usleep(10000);
	}
	return NULL;
}