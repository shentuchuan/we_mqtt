/*******************************************************************************
*  Copyright Statement:
*  --------------------
*  This software is protected by Copyright and the information contained
*  herein is confidential. The software may not be copied and the information
*  contained herein may not be used or disclosed except with the written
*  permission of Airoha Technology Corp. (C) 2021
*
*  BY OPENING THIS FILE, BUYER HEREBY UNEQUIVOCALLY ACKNOWLEDGES AND AGREES
*  THAT THE SOFTWARE/FIRMWARE AND ITS DOCUMENTATIONS ("AIROHA SOFTWARE")
*  RECEIVED FROM AIROHA AND/OR ITS REPRESENTATIVES ARE PROVIDED TO BUYER ON
*  AN "AS-IS" BASIS ONLY. AIROHA EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES,
*  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF
*  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE OR NONINFRINGEMENT.
*  NEITHER DOES AIROHA PROVIDE ANY WARRANTY WHATSOEVER WITH RESPECT TO THE
*  SOFTWARE OF ANY THIRD PARTY WHICH MAY BE USED BY, INCORPORATED IN, OR
*  SUPPLIED WITH THE AIROHA SOFTWARE, AND BUYER AGREES TO LOOK ONLY TO SUCH
*  THIRD PARTY FOR ANY WARRANTY CLAIM RELATING THERETO. AIROHA SHALL ALSO
*  NOT BE RESPONSIBLE FOR ANY AIROHA SOFTWARE RELEASES MADE TO BUYER'S
*  SPECIFICATION OR TO CONFORM TO A PARTICULAR STANDARD OR OPEN FORUM.
*
*  BUYER'S SOLE AND EXCLUSIVE REMEDY AND AIROHA'S ENTIRE AND CUMULATIVE
*  LIABILITY WITH RESPECT TO THE AIROHA SOFTWARE RELEASED HEREUNDER WILL BE,
*  AT AIROHA'S OPTION, TO REVISE OR REPLACE THE AIROHA SOFTWARE AT ISSUE,
*  OR REFUND ANY SOFTWARE LICENSE FEES OR SERVICE CHARGE PAID BY BUYER TO
*  AIROHA FOR SUCH AIROHA SOFTWARE AT ISSUE.
*
*  THE TRANSACTION CONTEMPLATED HEREUNDER SHALL BE CONSTRUED IN ACCORDANCE
*  WITH THE LAWS OF THE STATE OF CALIFORNIA, USA, EXCLUDING ITS CONFLICT OF
*  LAWS PRINCIPLES.  ANY DISPUTES, CONTROVERSIES OR CLAIMS ARISING THEREOF AND
*  RELATED THERETO SHALL BE SETTLED BY ARBITRATION IN SAN FRANCISCO, CA, UNDER
*  THE RULES OF THE INTERNATIONAL CHAMBER OF COMMERCE (ICC).
*
*******************************************************************************/

/* FILE NAME:  mqttd.c
 * PURPOSE:
 *  Implement MQTT client daemon for RTOS turnkey
 *
 * NOTES:
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mqttd.h"
#include "mqttd_queue.h"
#include "mw_error.h"
#include "lwip/ip.h"
#include "lwip/ip_addr.h"
#include "lwip/apps/mqtt.h"
#include "lwip/apps/mqtt_opts.h"
#include "lwip/apps/mqtt_priv.h"
#include "lwip/dns.h"
#include "lwip/netif.h"
#include "mbedtls/md5.h"
#include "osapi.h"
#include "osapi_timer.h"
#include "osapi_thread.h"
#include "osapi_string.h"
#include "osapi_mutex.h"
#include "db_api.h"
#include "db_data.h"
#include "inet_utils.h"
#include "web.h"
#include "hr_cjson.h"


/* NAMING CONSTANT DECLARATIONS
*/

/* MQTTD Client Daemon task
*/
#define MQTTD_TASK_NAME             "mqttd"
#define MQTTD_TIMER_NAME            "mqttdTmr"
#define MQTTD_STACK_SIZE            (808)
#define MQTTD_THREAD_PRI            (4)
#define MQTTD_MAX_TOPIC_SIZE        (64)   // Topic MAX Length 42 + tail end
#define MQTTD_TIMER_PERIOD          (500)
#define MQTTD_MUX_LOCK_TIME         (50)
#define MQTTD_MAX_REMAIN_MSG        (64)
#define MQTTD_MAX_BUFFER_SIZE       (64)
#define MQTTD_RECONNECT_TIMER_NAME  "mqttdReTmr"
#define MQTTD_TIMER_RECONNECT_PERIOD (1000)

/* MQTTD Client ID
*/
#define MQTTD_MAX_CLIENT_ID_SIZE     	(64)
#define MQTTD_MAX_TOPIC_PREFIX_SIZE    	(64)
#define MQTTD_MAX_SN_SIZE     			(17)
#define MQTTD_MAX_MAC_SIZE     			(19)
#define MQTTD_MAX_DEVICE_ID_SIZE     	(33)



#define MQTTD_CLIENT_CONNECT_MSG_FMT "{\"port_num\":%2d, \"msg_ver\":\"%s\"}"
#define MQTTD_MSG_VER                "2.0"

/* MQTTD Server Login
*/
//const ip_addr_t mqttd_server_ip = IPADDR4_INIT_BYTES(47, 237, 80, 17);  // for ikuai domain
const ip_addr_t mqttd_server_ip = IPADDR4_INIT_BYTES(192, 168, 0, 100);  // for ikuai domain

#if LWIP_DNS
const C8_T cloud_hostname[] = "swmgr.hruicloud.com";
#endif
#define MQTT_SRV_PORT			    (10883)
#define MQTTD_USERNAME              "ik_test"
#define MQTTD_PASSWD                "eiChaes7"
#define MQTTD_KEEP_ALIVE            (5)
#define MQTTD_RC4_KEY               "sqMVh5qAnHpLeMeM"

/* MQTTD Topics
*/
#define MQTTD_TOPIC_PREFIX          "hongrui"
#define MQTTD_TOPIC_CLOUD_PREFIX    "hongrui/sw"
//Will message
//#define MQTTD_WILL_TOPIC            MQTTD_TOPIC_PREFIX "/will"
#define MQTTD_WILL_QOS              (2)
#define MQTTD_WILL_RETAIN           (1)
//Normal message
//#define MQTTD_TOPIC_NEW             MQTTD_TOPIC_PREFIX "/new"
//#define MQTTD_TOPIC_DB              MQTTD_TOPIC_PREFIX "/db"
//#define MQTTD_TOPIC_INIT            MQTTD_TOPIC_DB "/init"
//#define MQTTD_TOPIC_DBRC            MQTTD_TOPIC_DB "/rc"
#define MQTTD_REQUEST_QOS           (0)
#define MQTTD_REQUEST_RETAIN        (0)
//Cloud message
//#define MQTTD_CLOUD_TODB            "mwcloud/db"
//#define MQTTD_SUB_CLOUD_FILTER      MQTTD_TOPIC_CLOUD_PREFIX "/#"
//#define MQTTD_CLOUD_CONNECTED       MQTTD_TOPIC_CLOUD_PREFIX "/connected"
//#define MQTTD_CLOUD_WILL            MQTTD_TOPIC_CLOUD_PREFIX "/will"
//#define MQTTD_CLOUD_CGI             MQTTD_TOPIC_CLOUD_PREFIX "/cgi"
//Normal Publish Topic format
/* <msg topic>/<cldb_ID>/<table_name>/<field_name>/eidx */
//#define MQTTD_TOPIC_FORMAT          "%s/%hu/%s/%s/%hu"

/* MQTTD Publish Application message
*/
#define MQTTD_MQX_OUTPUT_SIZE       (1024)                            /* the size * MQTT_REQ_MAX_IN_FLIGHT = MQTT_OUTPUT_RINGBUF_SIZE */
#define MQTTD_MAX_PACKET_SIZE       (MQTTD_MQX_OUTPUT_SIZE - 2 - 7)   /* MAX size of topic + payload, exclude the fix header and length */
#define MQTTD_MSG_HEADER_SIZE       (sizeof(MQTTD_PUB_MSG_T) - DB_MSG_PTR_SIZE)     /* size of message header and length in PUBLISH payload */
#define MQTTD_MAX_SESSION_ID        (255)
/* MACRO FUNCTION DECLARATIONS
 */

/* DATA TYPE DECLARATIONS
*/
/* The MQTTD state machine */
typedef enum {
    MQTTD_STATE_CONNECTING = 0,
    MQTTD_STATE_CONNECTED = 1,
    MQTTD_STATE_SUBSCRIBE = 2,
    MQTTD_STATE_SUBACK = 3,
    MQTTD_STATE_INITING = 4,
    MQTTD_STATE_RUN = 5,
    MQTTD_STATE_DISCONNECTED = 6,
    MQTTD_STATE_SHUTDOWN = 7
}MQTTD_STATE_T;

typedef MW_ERROR_NO_T (*queue_recv_t)(void **ptr_buf);
typedef MW_ERROR_NO_T (*queue_send_t)(const UI8_T method, const UI8_T t_idx, const UI8_T f_idx, const UI16_T e_idx, const void *ptr_data, const UI16_T size, DB_MSG_T **pptr_out_msg);
typedef MW_ERROR_NO_T (*queue_setData_t)(const UI8_T method, const UI8_T t_idx, const UI8_T f_idx, const UI16_T e_idx, const void *ptr_data, const UI16_T size);
typedef MW_ERROR_NO_T (*queue_getData_t)(const UI8_T in_t_idx, const UI8_T in_f_idx, const UI16_T in_e_idx, DB_MSG_T **pptr_out_msg, UI16_T *ptr_out_size, void **pptr_out_data);

/* The publish application message format */
// Obsolete
typedef struct MQTTD_PUB_MSG_OLD_S
{
    UI16_T          cldb_id;       /* The switch identifier in cloud DB */
    UI16_T          session_id;    /* The cloud request session identifier */
    UI8_T           method;        /* The method bitmap */
    union {
        UI8_T       count;         /* The data payload count in request or notification */
        UI8_T       result;        /* The response result with type MW_ERROR_NO_T */
    } type;
    DB_REQUEST_TYPE_T  request;    /* The type of the message */
    UI16_T          data_size;     /* The incoming data size */
    void*           ptr_data;
} ATTRIBUTE_PACK MQTTD_PUB_MSG_OLD_T;

typedef struct MQTTD_PUB_MSG_S
{
    UI8_T           pid;           /* The packet index of the whole message */
    UI8_T           count;         /* The data payload count in request or notification */
    UI8_T           method;        /* The method bitmap */
    UI16_T          data_size;     /* The data size */
    void*           ptr_data;
} ATTRIBUTE_PACK MQTTD_PUB_MSG_T;

typedef struct MQTTD_PUB_LIST_S
{
    C8_T            topic[MQTTD_MAX_TOPIC_SIZE];
    UI16_T          msg_size;
    void*           msg;
    struct MQTTD_PUB_LIST_S *next;
} ATTRIBUTE_PACK MQTTD_PUB_LIST_T;

/* The MQTTD ctrl structure */
typedef struct MQTTD_CTRL_S
{
    MQTTD_STATE_T   state;
    ip_addr_t       server_ip;
	UI16_T			port;
    UI16_T          cldb_id;        /* The switch identifier in cloud DB */
    C8_T            client_id[MQTTD_MAX_CLIENT_ID_SIZE];	/*{manufacturer}:sw:{deviceid}*/
	C8_T            topic_prefix[MQTTD_MAX_TOPIC_PREFIX_SIZE];	/*{manufacturer}/sw/{deviceid}*/
	C8_T			sn[MQTTD_MAX_SN_SIZE];
	C8_T			mac[MQTTD_MAX_MAC_SIZE];
	C8_T			device_id[MQTTD_MAX_DEVICE_ID_SIZE];
    mqtt_client_t*  ptr_client;
    UI8_T           db_subscribed;
    UI8_T           reconnect;
    C8_T            pub_in_topic[MQTTD_MAX_TOPIC_SIZE];
    UI8_T           remain_msgs;    /* Not yet sent message count */
    MQTTD_PUB_LIST_T *msg_head;
} ATTRIBUTE_PACK MQTTD_CTRL_T;

/* GLOBAL VARIABLE DECLARATIONS
*/
UI8_T mqttd_enable;
MQTTD_CTRL_T mqttd;

/* LOCAL SUBPROGRAM SPECIFICATIONS
*/
static void _mqttd_ctrl_init(MQTTD_CTRL_T *ptr_mqttd, ip_addr_t *server_ip);
static void _mqttd_ctrl_free(MQTTD_CTRL_T *ptr_mqttd);
static MW_ERROR_NO_T _mqttd_append_remain_msg(MQTTD_CTRL_T *ptr_mqttd, C8_T *topic, UI16_T msg_size, void *ptr_msg);
static void _mqttd_send_remain_msg(MQTTD_CTRL_T *ptr_mqttd);
static void _mqttd_tmr(timehandle_t ptr_xTimer);
/*=== DB related local functions ===*/
static void _mqttd_gen_client_id(MQTTD_CTRL_T *ptr_mqttd);
static MW_ERROR_NO_T _mqttd_subscribe_db(MQTTD_CTRL_T *ptr_mqttd);
static MW_ERROR_NO_T _mqttd_unsubscribe_db(MQTTD_CTRL_T *ptr_mqttd);
static void _mqttd_listen_db(MQTTD_CTRL_T *ptr_mqttd);
/*=== MQTT related local functions ===*/
static void _mqttd_cgi_proxy(MQTTD_CTRL_T *ptr_mqttd, const u8_t *data, u16_t len);
static void _mqttd_dataDump(const void *data, UI16_T data_size);
//static UI16_T _mqttd_db_topic_set(MQTTD_CTRL_T *ptr_mqttd, const UI8_T method, const UI8_T t_idx, const UI8_T f_idx, const UI16_T e_idx, C8_T *topic, UI16_T buf_size);
static void _mqttd_publish_cb(void *arg, err_t err);
static MW_ERROR_NO_T _mqttd_publish_data(MQTTD_CTRL_T *ptr_mqttd, const UI8_T method, C8_T *topic, const UI16_T data_size, const void *ptr_data);
static void _mqttd_incoming_publish_cb(void *arg, const char *topic, u32_t tot_len);
static void _mqttd_incoming_data_cb(void *arg, const u8_t *data, u16_t len, u8_t flags);
static void _mqttd_subscribe_cb(void *arg, err_t err);
static void _mqttd_send_subscribe(mqtt_client_t *client, void *arg);
static void _mqttd_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status);
#if LWIP_DNS
void _mqttd_dns_found(const char *name, const ip_addr_t *ipaddr, void *callback_arg);
#endif
static MW_ERROR_NO_T _mqttd_lookup_server(MQTTD_CTRL_T *ptr_mqttd);
static MW_ERROR_NO_T _mqttd_client_connect(MQTTD_CTRL_T *ptr_mqttd);
static void _mqttd_client_disconnect(mqtt_client_t* ptr_mqttclient);
static void _mqttd_main(void *arg);
MW_ERROR_NO_T _mqttd_deinit(void);
/*=== MQTT reconnect functions ===*/
static void _mqttd_reconnect_tmr(timehandle_t ptr_xTimer);
void mqttd_reconnect(void);

/* STATIC VARIABLE DECLARATIONS
 */
static threadhandle_t ptr_mqttdmain = NULL;
static timehandle_t ptr_mqttd_time = NULL;
static semaphorehandle_t ptr_mqttmutex = NULL;
static timehandle_t ptr_mqttd_recon_time = NULL;

void mqttd_rc4_encrypt(unsigned char *data, int data_len, const char *key, unsigned char *output) 
{
    int i, j = 0, k;
    unsigned char S[256];
    unsigned char temp;

    // Initialize the key-scheduling algorithm (KSA)
    for (i = 0; i < 256; i++) {
        S[i] = i;
    }

    for (i = 0; i < 256; i++) {
        j = (j + S[i] + key[i % strlen(key)]) % 256;
        temp = S[i];
        S[i] = S[j];
        S[j] = temp;
    }

    // Initialize the pseudo-random generation algorithm (PRGA)
    i = 0;
    j = 0;
    for (k = 0; k < data_len; k++) {
        i = (i + 1) % 256;
        j = (j + S[i]) % 256;
        temp = S[i];
        S[i] = S[j];
        S[j] = temp;
        output[k] = data[k] ^ S[(S[i] + S[j]) % 256];
    }
}

#define mqttd_rc4_decrypt mqttd_rc4_encrypt


/* LOCAL SUBPROGRAM BODIES
 */
/* FUNCTION NAME:  _mqttd_ctrl_init
 * PURPOSE:
 *      Initialize the control structure
 *
 * INPUT:
 *      ptr_mqttd  -- The control structure
 *      server_ip  -- The remote mqtt server ip
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
static void _mqttd_ctrl_init(MQTTD_CTRL_T *ptr_mqttd, ip_addr_t *server_ip)
{
    mqttd_debug("Initialize the MQTTD control structure.");

    ptr_mqttd->state = MQTTD_STATE_CONNECTING;
    ptr_mqttd->db_subscribed = FALSE;
    if ((server_ip != NULL) && (server_ip != &(ptr_mqttd->server_ip)))
    {
        osapi_memcpy((void *)&(ptr_mqttd->server_ip), (const void *)server_ip, sizeof(ip_addr_t));
    }
    ptr_mqttd->cldb_id = 0;
    ptr_mqttd->ptr_client = NULL;
    osapi_memset(ptr_mqttd->pub_in_topic, 0, MQTTD_MAX_TOPIC_SIZE);
    ptr_mqttd->remain_msgs = 0;
    ptr_mqttd->msg_head = NULL;
    ptr_mqttd->reconnect = FALSE;
}

/* FUNCTION NAME:  _mqttd_ctrl_free
 * PURPOSE:
 *      Free the control structure
 *
 * INPUT:
 *      ptr_mqttd  -- The control structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
static void _mqttd_ctrl_free(MQTTD_CTRL_T *ptr_mqttd)
{
    MQTTD_PUB_LIST_T *ptr_msg = NULL;
    MQTTD_PUB_LIST_T *ptr_head = ptr_mqttd->msg_head;

    mqttd_debug("Free the MQTTD control structure.");

#if MQTTD_SUPPORT_TLS
    if(NULL != tls_config)
    {
        altcp_tls_free_config(tls_config);
        tls_config = NULL;
    }
#endif

    while (ptr_mqttd->msg_head != NULL)
    {
        ptr_msg = ptr_mqttd->msg_head;
        osapi_free(ptr_msg->msg);
        osapi_free(ptr_mqttd->msg_head);
        ptr_mqttd->msg_head = ptr_msg->next;
        if (ptr_mqttd->msg_head == ptr_head)
        {
            /* The last message pointer*/
            ptr_mqttd->msg_head = NULL;
        }
    }
    ptr_mqttd->remain_msgs = 0;
    if (NULL != ptr_mqttd->ptr_client)
    {
        mqtt_client_free(ptr_mqttd->ptr_client);
    }
    ptr_mqttd->ptr_client = NULL;
    ptr_mqttd->cldb_id = 0;
    osapi_memset(ptr_mqttd->pub_in_topic, 0, MQTTD_MAX_TOPIC_SIZE);
}

/* FUNCTION NAME:  _mqttd_append_remain_msg
 * PURPOSE:
 *      If the MQTT request list is full, try to keep the msg in MQTTD
 *
 * INPUT:
 *      ptr_mqttd  -- The control structure
 *      topic      -- The publish topic
 *      msg_size   -- The publish message size
 *      ptr_msg    -- The publish message
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      MW_ERROR_OK
 *      MW_E_NO_MEMORY
 *
 * NOTES:
 *      None
 */
static MW_ERROR_NO_T _mqttd_append_remain_msg(MQTTD_CTRL_T *ptr_mqttd, C8_T *topic, UI16_T msg_size, void *ptr_msg)
{
    MW_ERROR_NO_T rc = MW_E_OK;
    MQTTD_PUB_LIST_T *ptr_temp = NULL;
    MQTTD_PUB_LIST_T *ptr_new = NULL;

    MW_CHECK_PTR(ptr_mqttd);
    MW_CHECK_PTR(topic);

    mqttd_debug("MQTTD append the remain message in list.");
    if (MQTTD_MAX_REMAIN_MSG <= ptr_mqttd->remain_msgs)
    {
        osapi_printf("MQTTD remain msg list is full.\n");
        return MW_E_NO_MEMORY;
    }

    if (MW_E_OK == osapi_mutexTake(ptr_mqttmutex, MQTTD_MUX_LOCK_TIME))
    {
        rc = osapi_calloc(sizeof(MQTTD_PUB_LIST_T), MQTTD_TASK_NAME, (void **)&(ptr_new));
        if (MW_E_OK != rc)
        {
            osapi_printf("%s: allocate memory failed(%d)\n", __func__, rc);
            osapi_mutexGive(ptr_mqttmutex);
            return MW_E_NO_MEMORY;
        }

        /* Assign data */
        osapi_strncpy(ptr_new->topic, topic, sizeof(ptr_new->topic));
        ptr_new->msg_size = msg_size;
        ptr_new->msg = ptr_msg;
        ptr_new->next = ptr_mqttd->msg_head;

        if (ptr_mqttd->msg_head == NULL)
        {
            ptr_mqttd->msg_head = ptr_new;
            ptr_mqttd->msg_head->next = ptr_mqttd->msg_head;
            mqttd_debug("Head not exist, create the MQTTD msg_head(%p)->next(%p).", ptr_mqttd->msg_head, ptr_mqttd->msg_head->next);
        }
        else
        {
            /* Find the tail */
            ptr_temp = ptr_mqttd->msg_head;
            while (ptr_mqttd->msg_head != ptr_temp->next)
            {
                mqttd_debug("Search the tail: temp(%p)->next(%p).", ptr_temp, ptr_temp->next);
                ptr_temp = ptr_temp->next;
            }
            ptr_temp->next = ptr_new;
        }

        ptr_mqttd->remain_msgs++;
        osapi_mutexGive(ptr_mqttmutex);
    }
    mqttd_debug("MQTTD append the remain message done.");
    return rc;
}

/* FUNCTION NAME:  _mqttd_send_remain_msg
 * PURPOSE:
 *      Try to send the remained message in MQTTD
 *
 * INPUT:
 *      ptr_mqttd  -- The control structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
static void _mqttd_send_remain_msg(MQTTD_CTRL_T *ptr_mqttd)
{
    err_t err;
    MQTTD_PUB_LIST_T *ptr_msg = NULL;
    MQTTD_PUB_LIST_T *ptr_head = NULL;
    mqttd_debug("Send the remain message to cloud.");

    /* Try to PUBLISH data ASAP */

    if (0 == ptr_mqttd->remain_msgs)
    {
        mqttd_debug("No remain message to send.");
        return;
    }
    if (MW_E_OK == osapi_mutexTake(ptr_mqttmutex, MQTTD_MUX_LOCK_TIME))
    {
        ptr_head = ptr_mqttd->msg_head;
        while (ptr_mqttd->msg_head != NULL)
        {
            if (ptr_mqttd->state > MQTTD_STATE_RUN)
            {
                break;
            }
            ptr_msg = ptr_mqttd->msg_head;
            err = mqtt_publish(ptr_mqttd->ptr_client, ptr_msg->topic, (const void *)ptr_msg->msg, ptr_msg->msg_size, MQTTD_REQUEST_QOS, MQTTD_REQUEST_RETAIN, _mqttd_publish_cb, (void *)ptr_mqttd);
            if (ERR_OK == err)
            {
                mqttd_debug("Send the MQTTD list(%p)->msg(%p).", ptr_msg, ptr_msg->msg);
                /* Already in MQTT request list, so the allocate memory can be free */
                osapi_free(ptr_msg->msg);
                ptr_msg->msg = NULL;
                ptr_mqttd->msg_head = ptr_msg->next;
                if (ptr_msg->next == ptr_head)
                {
                    /* The last message sent */
                    ptr_mqttd->msg_head = NULL;
                }
                osapi_free(ptr_msg);
                ptr_msg = NULL;
                ptr_mqttd->remain_msgs--;
            }
            else if (ERR_CONN == err)
            {
                mqttd_debug("Error (%d): The MQTT connection is broken", err);
                ptr_mqttd->state = MQTTD_STATE_DISCONNECTED;
                break;
            }
            else
            {
                mqttd_debug("Error (%d): Send the MQTTD list(%p)->msg(%p).", err, ptr_msg, ptr_msg->msg);
                break;
            }
        }

        /* Re-link the ring list */
        if ((ptr_mqttd->msg_head != NULL) && (ptr_mqttd->msg_head != ptr_head))
        {
            while (ptr_head != ptr_msg->next)
            {
                ptr_msg = ptr_msg->next;
            }
            ptr_msg->next = ptr_mqttd->msg_head;
        }

        osapi_mutexGive(ptr_mqttmutex);
    }
}


/* FUNCTION NAME:  _mqttd_tmr
 * PURPOSE:
 *      The timer process
 *
 * INPUT:
 *      ptr_xTimer
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
static void _mqttd_tmr(timehandle_t ptr_xTimer)
{
    if (mqttd.state == MQTTD_STATE_RUN)
    {
        _mqttd_send_remain_msg(&mqttd);
    }
}
/*=== DB related local functions ===*/

#if 0
/* FUNCTION NAME:  _mqttd_gen_client_id
 * PURPOSE:
 *      Get the hardware verion and MAC address of the device
 *
 * INPUT:
 *      ptr_mqttd  --  the pointer of MQTTD ctrl structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      The string length of generated client_id
 *
 * NOTES:
 *      None
 */
static UI8_T _mqttd_gen_client_id(MQTTD_CTRL_T *ptr_mqttd)
{
    MW_ERROR_NO_T rc = MW_E_OK;
    MW_MAC_T sys_mac;
    C8_T hw_version[MAX_VERSION_SIZE] = {0};
    C8_T mac_string[13] = {0};
    DB_MSG_T *db_msg = NULL;
    UI16_T db_size = 0;
    void *db_data = NULL;

    /* get hardware version ex. EN8851*/
    rc = mqttd_queue_getData(SYS_OPER_INFO, SYS_OPER_HW_VER, DB_ALL_ENTRIES, &db_msg, &db_size, &db_data);
    if(MW_E_OK == rc)
    {
        mqttd_debug_db("get hw_version success, ptr_msg =%p", db_msg);
        memcpy(hw_version, db_data, db_size);
        osapi_free(db_msg);
    }
    else
    {
        mqttd_debug_db("%s", "get hw_version failed");
        osapi_snprintf(hw_version, sizeof(hw_version), "%s", MQTTD_CLIENT_ID_PRODUCT_DFT);
    }

    /* get sys_mac */
    osapi_memset(sys_mac, 0, sizeof(sys_mac));
    rc = mqttd_queue_getData(SYS_OPER_INFO, SYS_OPER_MAC, DB_ALL_ENTRIES, &db_msg, &db_size, &db_data);
    if(MW_E_OK == rc)
    {
        mqttd_debug_db("get sys_mac success, ptr_msg =%p", db_msg);
        memcpy(sys_mac, db_data, db_size);
        osapi_free(db_msg);
    }
    else
    {
        mqttd_debug_db("%s", "get sys_mac failed");
    }

    osapi_snprintf(mac_string, sizeof(mac_string), "%02x%02x%02x%02x%02x%02x",
        sys_mac[0], sys_mac[1], sys_mac[2], sys_mac[3], sys_mac[4], sys_mac[5]);

    osapi_snprintf(ptr_mqttd->client_id, MQTTD_MAX_CLIENT_ID_SIZE, MQTTD_CLIENT_ID_FMT, hw_version, mac_string);
    mqttd_debug("Create MQTTD client id: %s", ptr_mqttd->client_id);
    return osapi_strlen(ptr_mqttd->client_id);
}
#endif
/* FUNCTION NAME:  _mqttd_subscribe_db
 * PURPOSE:
 *      Subscribe all internal DB tables
 *
 * INPUT:
 *      ptr_mqttd  --  the pointer of MQTTD ctrl structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      MW_E_OK
 *      MW_E_OP_INCOMPLETE
 *
 * NOTES:
 *      None
 */
static MW_ERROR_NO_T
_mqttd_subscribe_db(
    MQTTD_CTRL_T *ptr_mqttd)
{
    MW_ERROR_NO_T rc = MW_E_OK;
    DB_MSG_T *ptr_msg = NULL;
    UI8_T *ptr_data = NULL;
    UI32_T msg_size = (DB_MSG_HEADER_SIZE + (TABLES_LAST * DB_MSG_PAYLOAD_SIZE));
    UI16_T offset= 0;
    UI8_T db_tidx = 0;

    mqttd_debug_db("Subscirbe internal DB");

    if (TRUE == ptr_mqttd->db_subscribed)
    {
        return MW_E_OK;
    }
    if (ptr_mqttd->state == MQTTD_STATE_DISCONNECTED)
    {
        return MW_E_OP_INVALID;
    }

    /* create the subscribe data payload */
    rc = osapi_calloc(msg_size, MQTTD_QUEUE_NAME, (void **)(&ptr_msg));
    if (MW_E_OK != rc)
    {
        mqttd_debug("Failed to allocate memory(rc = %u)", rc);
        return rc;
    }

    offset = dbapi_setMsgHeader(ptr_msg, MQTTD_QUEUE_NAME, M_SUBSCRIBE, TABLES_LAST);
    ptr_data = (UI8_T *)ptr_msg;
    for (db_tidx = 0; db_tidx < TABLES_LAST; db_tidx++)
    {
        /* message header */
        offset += dbapi_setMsgPayload(M_SUBSCRIBE, db_tidx, DB_ALL_FIELDS, DB_ALL_ENTRIES, NULL, ptr_data + offset);
    }

    /* send request */
    rc = dbapi_sendMsg(ptr_msg, MQTTD_MUX_LOCK_TIME);
    if (MW_E_OK != rc)
    {
        mqttd_debug_db("Failed to send message to DB Queue");
        osapi_free(ptr_msg);
        return rc;
    }
    mqttd_debug_db("Subscribe DB success, db_msg =%p", ptr_msg);
    ptr_mqttd->db_subscribed = TRUE;
    return rc;
}

/* FUNCTION NAME:  _mqttd_unsubscribe_db
 * PURPOSE:
 *      Unsubscribe all internal DB tables
 *
 * INPUT:
 *      ptr_mqttd  --  the pointer of MQTTD ctrl structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      MW_E_OK
 *      MW_E_OP_INCOMPLETE
 *
 * NOTES:
 *      None
 */
static MW_ERROR_NO_T
_mqttd_unsubscribe_db(
    MQTTD_CTRL_T *ptr_mqttd)
{
    MW_ERROR_NO_T rc = MW_E_OK;
    DB_MSG_T *ptr_msg = NULL;
    UI8_T *ptr_data = NULL;
    UI32_T msg_size = (DB_MSG_HEADER_SIZE + (TABLES_LAST * DB_MSG_PAYLOAD_SIZE));
    UI16_T offset= 0;
    UI8_T db_tidx = 0;

    mqttd_debug_db("Unsubscirbe internal DB");

    if (FALSE == ptr_mqttd->db_subscribed)
    {
        return MW_E_OK;
    }

    /* create the subscribe data payload */
    rc = osapi_calloc(msg_size, MQTTD_QUEUE_NAME, (void **)(&ptr_msg));
    if (MW_E_OK != rc)
    {
        mqttd_debug_db("Failed to allocate memory(rc = %u)", rc);
        return rc;
    }

    offset = dbapi_setMsgHeader(ptr_msg, MQTTD_QUEUE_NAME, M_UNSUBSCRIBE, TABLES_LAST);
    ptr_data = (UI8_T *)ptr_msg;
    for (db_tidx = 0; db_tidx < TABLES_LAST; db_tidx++)
    {
        /* message header */
        offset += dbapi_setMsgPayload(M_UNSUBSCRIBE, db_tidx, DB_ALL_FIELDS, DB_ALL_ENTRIES, NULL, ptr_data + offset);
    }

    /* send request */
    rc = dbapi_sendMsg(ptr_msg, MQTTD_MUX_LOCK_TIME);
    if (MW_E_OK != rc)
    {
        mqttd_debug_db("Failed to send message to DB Queue");
        osapi_free(ptr_msg);
        return rc;
    }
    mqttd_debug_db("Unsubscribe DB success, db_msg =%p", ptr_msg);
    ptr_mqttd->db_subscribed = FALSE;
    return rc;
}

/*publish sysinfo to mqtt cloud server with event topic*/
static MW_ERROR_NO_T _mqttd_publish_sysinfo(MQTTD_CTRL_T *ptr_mqttd,  const void *ptr_data)
{
	MW_ERROR_NO_T rc = MW_E_OK;

	return rc;
}

#if 0
/* FUNCTION NAME:  _mqttd_listen_db
 * PURPOSE:
 *      Listen to DB's notification
 *
 * INPUT:
 *      ptr_mqttd    --  the pointer of MQTTD ctrl structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *      None
 */
static void
_mqttd_listen_db(
    MQTTD_CTRL_T *ptr_mqttd)
{
    MW_ERROR_NO_T rc = MW_E_OK;
    C8_T topic[MQTTD_MAX_TOPIC_SIZE] = {0};
    DB_MSG_T *ptr_msg = NULL;
    UI8_T *ptr_data = NULL;
    UI16_T msg_size = 0;
    UI8_T count = 0;
    UI8_T method = 0;
    DB_REQUEST_TYPE_T req;

    /* Block mode to receive the DB message */
    rc = dbapi_recvMsg(
            MQTTD_QUEUE_NAME,
            &ptr_msg,
            MQTTD_QUEUE_TIMEOUT);
    if (MW_E_OK != rc)
    {
        return;
    }

    mqttd_debug_db("get ptr_msg =%p", ptr_msg);
    if (M_B_RESPONSE == (ptr_msg->method & M_B_RESPONSE))
    {
        mqttd_debug_db("free the response meesage: ptr_msg =%p", ptr_msg);
        osapi_free(ptr_msg);
        return;
    }

    /* Send the updates to Cloud */
    if (ptr_mqttd->ptr_client != NULL)
    {
        count = ptr_msg->type.count;
        mqttd_debug_db("count =%d", count);
        if (M_GET == ptr_msg->method)
        {
            method = M_CREATE;
        }
        else
        {
            method = ptr_msg->method;
        }
        mqttd_debug_db("method =%u", method);
        ptr_data = (UI8_T *)&(ptr_msg->ptr_payload);
        while (count > 0)
        {
            memcpy((void *)&req, (const void *)ptr_data, sizeof(DB_REQUEST_TYPE_T));
            ptr_data += sizeof(DB_REQUEST_TYPE_T);
            mqttd_debug_db("T/F/E =%u/%u/%u", req.t_idx, req.f_idx, req.e_idx);
            /* append db json data to PUBLISH request list */
            memcpy((void *)&msg_size, (const void *)ptr_data, sizeof(UI16_T));
            mqttd_debug_db("data size =%u", msg_size);
            ptr_data += sizeof(UI16_T);
            /* append db rawdata to PUBLISH request list */
            if (0 != _mqttd_db_topic_set(ptr_mqttd, method, req.t_idx, req.f_idx, req.e_idx, topic, MQTTD_MAX_TOPIC_SIZE))
            {
                /* append db rawdata to PUBLISH request list */
                (void)_mqttd_publish_data(ptr_mqttd, method, topic, msg_size, ptr_data);
            }
            else
            {
                mqttd_debug_db("Cannot send data: [T/F/E] %u/%u/%u", req.t_idx, req.f_idx, req.e_idx);
            }
            count--;
            ptr_data += msg_size;
        }
    }
    mqttd_debug_db("free ptr_msg =%p\n", ptr_msg);
    osapi_free(ptr_msg);
    ptr_msg = NULL;
}

#else
/* FUNCTION NAME:  _mqttd_listen_db
 * PURPOSE:
 *      Listen to DB's notification
 *
 * INPUT:
 *      ptr_mqttd    --  the pointer of MQTTD ctrl structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *      None
 */
static void
_mqttd_listen_db(
    MQTTD_CTRL_T *ptr_mqttd)
{
    MW_ERROR_NO_T rc = MW_E_OK;
    C8_T topic[MQTTD_MAX_TOPIC_SIZE] = {0};
    DB_MSG_T *ptr_msg = NULL;
    UI8_T *ptr_data = NULL;
    UI16_T msg_size = 0;
    UI8_T count = 0;
    UI8_T method = 0;
    DB_REQUEST_TYPE_T req;

    /* Block mode to receive the DB message */
    rc = dbapi_recvMsg(
            MQTTD_QUEUE_NAME,
            &ptr_msg,
            MQTTD_QUEUE_TIMEOUT);
    if (MW_E_OK != rc)
    {
        return;
    }

    mqttd_debug_db("get ptr_msg =%p", ptr_msg);
    if (M_B_RESPONSE == (ptr_msg->method & M_B_RESPONSE))
    {
        mqttd_debug_db("free the response meesage: ptr_msg =%p", ptr_msg);
        osapi_free(ptr_msg);
        return;
    }

    /* Send the updates to Cloud */
    if (ptr_mqttd->ptr_client != NULL)
    {
        count = ptr_msg->type.count;
        method = ptr_msg->method;
        mqttd_debug_db("count =%d, method =%u", count, method);

        if (M_UPDATE == method)
        {
            ptr_data = (UI8_T *)&(ptr_msg->ptr_payload);
            while (count > 0)
            {
                memcpy((void *)&req, (const void *)ptr_data, sizeof(DB_REQUEST_TYPE_T));
                ptr_data += sizeof(DB_REQUEST_TYPE_T);
                mqttd_debug_db("T/F/E =%u/%u/%u", req.t_idx, req.f_idx, req.e_idx);
                /* append db json data to PUBLISH request list */
                memcpy((void *)&msg_size, (const void *)ptr_data, sizeof(UI16_T));
                mqttd_debug_db("data size =%u", msg_size);
                ptr_data += sizeof(UI16_T);
                /* append db rawdata to PUBLISH request list */
                switch (req.t_idx)  /*TABLES_T*/
                {
                    case SYS_INFO:
						(void)_mqttd_publish_sysinfo(ptr_mqttd, ptr_data);
                        break;
                    case ACCOUNT_INFO:
                        break;
                    case PORT_CFG_INFO:
                        break;
                    case LOGON_INFO:
                        break;
                    case PORT_MIRROR_INFO:
                        break;
                    default:
                        mqttd_debug_db("Invalid TABLES_T value: %u", req.t_idx);
                        break;
                }
                count--;
                ptr_data += msg_size;
            }
        }
    }
    mqttd_debug_db("free ptr_msg =%p\n", ptr_msg);
    osapi_free(ptr_msg);
    ptr_msg = NULL;
}

#endif
/* FUNCTION NAME:  _mqttd_db_proxy
 * PURPOSE:
 *      Proxy the mqtt request to internal DB
 *
 * INPUT:
 *      ptr_mqttd  --  the pointer of MQTTD ctrl structure
 *      data       --  incoming data
 *      len        --  incoming data length
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *      Only support RAWDATA
 */
static void _mqttd_db_proxy(MQTTD_CTRL_T *ptr_mqttd, const u8_t *data, u16_t len)
{
    MW_ERROR_NO_T rc = MW_E_OK;
    MQTTD_PUB_MSG_OLD_T *ptr_msg = (MQTTD_PUB_MSG_OLD_T *)data;
    UI8_T *ptr_data = NULL;
    DB_MSG_T *ptr_db_msg = NULL;
    void *ptr_db_data = NULL;
    DB_REQUEST_TYPE_T req;
    UI32_T msg_size = 0;
    UI16_T data_size = 0;
    UI16_T offset = 0;
    UI8_T count = 0;

    mqttd_debug("Proxy the cloud PUBLISH request to DB.");

    if ((ptr_mqttd == NULL) || (len == 0))
    {
        mqttd_debug("Argument error.");
        return;
    }
    if (ptr_msg->cldb_id != ptr_mqttd->cldb_id)
    {
        mqttd_debug("The message is not for me.");
        return;
    }

    /* message */
    mqttd_debug_pkt("Incoming session_id=%u", ptr_msg->session_id);
    mqttd_debug_pkt("Incoming method=0x%X", ptr_msg->method);
    mqttd_debug_pkt("Incoming count=%u", ptr_msg->type.count);

    count = ptr_msg->type.count;
    ptr_data = (UI8_T *)&(ptr_msg->request);
    while (count > 0)
    {
        ptr_data += sizeof(DB_REQUEST_TYPE_T);
        data_size = ((*(ptr_data+1) << 8) | (*ptr_data));
        msg_size += data_size;
        ptr_data += sizeof(UI16_T);
        _mqttd_dataDump((const void *)ptr_data, data_size);
        mqttd_debug_pkt("Incoming t_idx/f_idx/e_idx=%u/%u/%u", req.t_idx, req.f_idx, req.e_idx);
        mqttd_debug_pkt("Incoming data_size=%u", data_size);
        count--;
    }
    msg_size += DB_MSG_HEADER_SIZE;

    /* allocate the message and send to internal DB */
    rc = osapi_calloc(msg_size, MQTTD_QUEUE_NAME, (void **)&ptr_db_msg);
    if (MW_E_OK != rc)
    {
        osapi_printf("%s: allocate memory failed(%d)\n", __func__, rc);
        return;
    }

    count = ptr_msg->type.count;
    offset = dbapi_setMsgHeader(ptr_db_msg, MQTTD_QUEUE_NAME, ptr_msg->method, count);
    ptr_db_data = (void *)ptr_db_msg;
    ptr_data = (UI8_T *)&(ptr_msg->request);
    while (count > 0)
    {
        memcpy((void *)&req, (const void *)&ptr_data, sizeof(DB_REQUEST_TYPE_T));
        ptr_data += sizeof(DB_REQUEST_TYPE_T);
        data_size = ((*(ptr_data+1) << 8) | (*ptr_data));
        ptr_data += sizeof(UI16_T);
        offset += dbapi_setMsgPayload(ptr_msg->method, req.t_idx, req.f_idx, req.e_idx, ptr_data, ptr_db_data + offset);
        ptr_data += data_size;
        count--;
    }

    /* send request */
    rc = dbapi_sendMsg(ptr_db_msg, MQTTD_MUX_LOCK_TIME);
    if (MW_E_OK != rc)
    {
        osapi_free(ptr_db_msg);
    }
}

/**
 * Extract URI parameters from the parameter-part of an URI in the form
 * "test.cgi?x=y" @todo: better explanation!
 * This function refers to httpd.c extract_uri_parameters function
 * and modifies the incoming buffer directly.
 *
 * @param params pointer to the NULL-terminated parameter string from the URI
 * @return number of parameters extracted
 */
static I32_T extract_uri_parameters(C8_T *params, C8_T **ptr_param, C8_T **ptr_param_val)
{
    C8_T *pair;
    C8_T *equals;
    I32_T loop;

    /* If we have no parameters at all, return immediately. */
    if (!params || (params[0] == '\0'))
    {
        return (0);
    }

    /* Get a pointer to our first parameter */
    pair = params;

    /* Parse up to LWIP_HTTPD_MAX_CGI_PARAMETERS from the passed string and ignore the
     * remainder (if any) */
    for (loop = 0; (loop < LWIP_HTTPD_MAX_CGI_PARAMETERS) && pair; loop++)
    {
        /* Save the name of the parameter */
        ptr_param[loop] = pair;

        /* Remember the start of this name=value pair */
        equals = pair;

        /* Find the start of the next name=value pair and replace the delimiter
         * with a 0 to terminate the previous pair string. */
        pair = strchr(pair, '&');
        if (pair)
        {
            *pair = '\0';
            pair++;
        }
        else
        {
            /* We didn't find a new parameter so find the end of the URI and
             * replace the space with a '\0' */
            pair = strchr(equals, ' ');
            if (pair)
            {
                *pair = '\0';
            }

            /* Revert to NULL so that we exit the loop as expected. */
            pair = NULL;
        }

        /* Now find the '=' in the previous pair, replace it with '\0' and save
         * the parameter value string. */
        equals = strchr(equals, '=');
        if (equals)
        {
            *equals = '\0';
            ptr_param_val[loop] = equals + 1;
        }
        else
        {
            ptr_param_val[loop] = NULL;
        }
    }

    return loop;
}

/* FUNCTION NAME:  _mqttd_cgi_proxy
 * PURPOSE:
 *      Proxy the mqtt request to CGI function
 *
 * INPUT:
 *      ptr_mqttd  --  the pointer of MQTTD ctrl structure
 *      data       --  incoming data
 *      len        --  incoming data length
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *      Only support MW html/cgi
 */
static void _mqttd_cgi_proxy(MQTTD_CTRL_T *ptr_mqttd, const u8_t *data, u16_t len)
{
    C8_T * uri = NULL;
    C8_T * params = NULL;
    C8_T * buff_end = NULL;
    const tCGI *mqttd_cgis = CGIURLs;
    UI16_T mqttd_cgi_num = get_numCgiHandler();
    UI16_T i = 0;
    I32_T  cgi_paramcount;
    C8_T *ptr_params[LWIP_HTTPD_MAX_CGI_PARAMETERS]; /* Params extracted from the request URI */
    C8_T *ptr_param_vals[LWIP_HTTPD_MAX_CGI_PARAMETERS]; /* Values for each extracted param */

    if ((ptr_mqttd == NULL) || (data == NULL))
    {
        return;
    }
    if ((len > 0) && (mqttd_cgi_num > 0))
    {
        uri = (C8_T *)data;
        buff_end = (C8_T *)&ptr_mqttd->ptr_client->rx_buffer[MQTT_VAR_HEADER_BUFFER_LEN];
        if ((uri+len) <= buff_end)
        {
            uri[len] = '\0';
        }
        else
        {
            uri[len - 1] = '\0';
        }
        mqttd_debug_pkt("Incoming request: %s", uri);
        params = (char *)strchr(uri, '?');
        if (params != NULL) {
            /* URI contains parameters. NULL-terminate the base URI */
            *params = '\0';
            params++;
        }

        for (i = 0; i < mqttd_cgi_num; i++)
        {
            if (osapi_strcmp(uri, mqttd_cgis[i].pcCGIName) == 0)
            {
                /*
                 * We found a CGI that handles this URI so extract the
                 * parameters and call the handler.
                 */
                cgi_paramcount = extract_uri_parameters(params, ptr_params, ptr_param_vals);
                /* CGI handle function */
                mqttd_debug_pkt("handle=[%d] paramCnt=[%d]", i, cgi_paramcount);
                if (cgiMutex)
                {
                    if (MW_E_OK == osapi_mutexTake(cgiMutex, MQTTD_MUX_LOCK_TIME))
                    {
                        mqttd_cgis[i].pfnCGIHandler(i, cgi_paramcount, ptr_params, ptr_param_vals);
                        osapi_mutexGive(cgiMutex);
                    }
                    mqttd_debug_pkt("CGI Done");
                }
                else
                {
                    mqttd_debug_pkt("CGI not support");
                }
                break;
            }
        }
    }
}

/*=== MQTT related local functions ===*/
/* FUNCTION NAME: _mqttd_dataDump
 * PURPOSE:
 *       Dump raw data if debug level is greater than MQTT_DEBUG_DISABLE
 *
 * INPUT:
 *       data       -- The raw data
 *       daia_size  -- The size of raw data
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *
 */
static void _mqttd_dataDump(const void *data, UI16_T data_size)
{
    UI32_T idx = 0, i = 0;
    I32_T count = 0;
    UI8_T *ptr_raw = (UI8_T *)data;
    C8_T rawdata[4] = {0};
    C8_T rawstr[MQTTD_MAX_BUFFER_SIZE] = {0};

    if ((mqttd_debug_level != MQTTD_DEBUG_ALL) || (data == NULL))
    {
        return;
    }
    while (idx < data_size)
    {
        memset(rawstr, 0, sizeof(rawstr));
        osapi_snprintf(rawstr, MQTTD_MAX_BUFFER_SIZE, "%04X: ", idx);
        for (i = 0; i < 16; i++)
        {
            count = osapi_sprintf(rawdata, " %02X", *ptr_raw);
            if (count <= 0)
            {
                idx = data_size;
                break;
            }
            strncat(rawstr, rawdata, count);
            ptr_raw++;
            idx++;
            if (idx >= data_size)
            {
                break;
            }
        }
        osapi_printf("    %s\n", rawstr);
    }
}

#if 0
/* FUNCTION NAME: _mqttd_db_topic_set
 * PURPOSE:
 *      Set the DB notification to MQTT message topic
 *
 * INPUT:
 *      ptr_mqttd       -- The control structure
 *      method          --  the method bitmap
 *      t_idx           --  the enum of the table
 *      f_idx           --  the enum of the field
 *      e_idx           --  the entry index in the table
 *      buf_size        --  the topic buffer size
 *
 * OUTPUT:
 *      topic           --  the topic to be sent
 *
 * RETURN:
 *      topic_length    -- the string legnth of topic
 *
 * NOTES:
 *
 */
static UI16_T _mqttd_db_topic_set(MQTTD_CTRL_T *ptr_mqttd, const UI8_T method, const UI8_T t_idx, const UI8_T f_idx, const UI16_T e_idx, C8_T *topic, UI16_T buf_size)
{
    I32_T length = 0;
    C8_T table_name[DB_MAX_KEY_SIZE] = {0};
    C8_T field_name[DB_MAX_KEY_SIZE] = {0};

    MW_PARAM_CHK((topic == NULL), 0);
    MW_PARAM_CHK((buf_size == 0), 0);

    if (MW_E_OK != dbapi_getTableName(t_idx, DB_MAX_KEY_SIZE, table_name))
    {
        return 0;
    }
    if (MW_E_OK != dbapi_getFieldName(t_idx, f_idx, DB_MAX_KEY_SIZE, field_name))
    {
        return 0;
    }

    /* The topic:
     * <msg topic>/<cldb_ID>/<table_name>/<field_name>/eidx
     */
    if (method == M_CREATE)
    {
        //PUBLISH the inital data
        length = osapi_snprintf(topic, buf_size, MQTTD_TOPIC_FORMAT,
                MQTTD_TOPIC_INIT, ptr_mqttd->cldb_id, table_name, field_name, e_idx);
    }
    else
    {
        //PUBLISH the internal DB notification
        length = osapi_snprintf(topic, buf_size, MQTTD_TOPIC_FORMAT,
                 MQTTD_TOPIC_DB, ptr_mqttd->cldb_id, table_name, field_name, e_idx);
    }
    if (length <= 0)
    {
        return 0;
    }
    mqttd_debug_db("Set topic: %s(len = %d).\n", topic, length);
    return (UI16_T)(length & 0xFFFF);
}

#endif
/* FUNCTION NAME: _mqttd_publish_cb
 * PURPOSE:
 *      MQTTD publish callback function
 *
 * INPUT:
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *
 */
static void _mqttd_publish_cb(void *arg, err_t err)
{
    mqttd_debug("Send Publish control packet code: (%d).\n", err);
}

/* FUNCTION NAME: _mqttd_publish_data
 * PURPOSE:
 *      Generate the data payload and PUBLISH
 *
 * INPUT:
 *      ptr_mqttd       -- The control structure
 *      method          --  the method bitmap
 *      topic           --  the message topic
 *      data_size       --  the message data size
 *      ptr_data        --  pointer to message data
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      MW_E_OK
 *      MW_E_BAD_PARAMETER
 *      MW_E_NO_MEMORY
 *      MW_E_OP_INVALID
 *
 * NOTES:
 *
 */
static MW_ERROR_NO_T _mqttd_publish_data(MQTTD_CTRL_T *ptr_mqttd, const UI8_T method, C8_T *topic, const UI16_T data_size, const void *ptr_data)
{
    MW_ERROR_NO_T   rc = MW_E_OK;
    MQTTD_PUB_MSG_T *ptr_msg = NULL;
    UI8_T           *pdata = (UI8_T *)ptr_data;
    UI8_T           pkt_idx = 1;
    UI8_T           pkt_count = 1;
    UI16_T          topic_size = 0;
    UI16_T          msg_size = 0;
    UI16_T          total_size = 0;
    UI16_T          sent_size = data_size;

    MW_CHECK_PTR(ptr_mqttd);
    MW_CHECK_PTR(topic);

    if (ptr_mqttd->state == MQTTD_STATE_DISCONNECTED)
    {
        return MW_E_OP_INVALID;
    }
    mqttd_debug_pkt("Publish topic: %s", topic);

    /* Calculate the packet size */
    topic_size = osapi_strlen(topic);
    msg_size = MQTTD_MSG_HEADER_SIZE + sent_size;
    total_size = topic_size + msg_size;
    if (total_size > MQTTD_MAX_PACKET_SIZE)
    {
        msg_size = MQTTD_MAX_PACKET_SIZE - topic_size;
        sent_size = msg_size - MQTTD_MSG_HEADER_SIZE;
        pkt_count = (data_size / sent_size) + 1;
    }

    do
    {
        ptr_msg = NULL;
        /* allocate the message payload memory */
        rc = osapi_calloc(msg_size, MQTTD_TASK_NAME, (void **)&ptr_msg);
        if (MW_E_OK != rc)
        {
            osapi_printf("%s: allocate memory failed(%d)\n", __func__, rc);
            return MW_E_NO_MEMORY;
        }
        mqttd_debug("ptr_msg=%p", ptr_msg);

        /* message */
        ptr_msg->pid = pkt_idx;
        ptr_msg->count = pkt_count;
        ptr_msg->method = method;
        ptr_msg->data_size = sent_size;
        mqttd_debug_pkt("Publish pid/count: %u/%u", ptr_msg->pid, ptr_msg->count);
        mqttd_debug_pkt("Publish method=0x%X", ptr_msg->method);
        mqttd_debug_pkt("Publish data_size=%u", ptr_msg->data_size);

        if ((sent_size > 0) && (pdata != NULL))
        {
            /* copy the data to buffer */
            _mqttd_dataDump((const void *)pdata, sent_size);
            memcpy(&(ptr_msg->ptr_data), pdata, sent_size);
            pdata += sent_size;
        }

        /* Send PUBLISH message directly if no remain msg */
        if (ptr_mqttd->msg_head == NULL)
        {
            err_t err = mqtt_publish(ptr_mqttd->ptr_client, topic, (const void *)ptr_msg, msg_size, MQTTD_REQUEST_QOS, MQTTD_REQUEST_RETAIN, _mqttd_publish_cb, (void *)ptr_mqttd);
            if (ERR_OK == err)
            {
                mqttd_debug("PUBLISH directly, free the memory ptr_msg (%p)", ptr_msg);
                /* Already in MQTT request list, so the allocated memory can be free */
                osapi_free(ptr_msg);
                ptr_msg = NULL;
                rc = MW_E_OK;
            }
            else
            {
                mqttd_debug("Error (%d): PUBLISH failed: ptr_msg(%p).", err, ptr_msg);
                rc = MW_E_OP_INCOMPLETE;
            }
        }
        else
        {
            rc = MW_E_OP_INCOMPLETE;
        }

        if (MW_E_OK != rc)
        {
            /* Try to keep the data, but if the memory is full, then the packet will be lost */
            if (MW_E_OK != _mqttd_append_remain_msg(ptr_mqttd, (C8_T *)topic, msg_size, (void *)ptr_msg))
            {
                osapi_free(ptr_msg);
                ptr_msg = NULL;
                return MW_E_NO_MEMORY;
            }
        }

        if (pkt_idx == pkt_count)
        {
            /* All data sent or keep */
            break;
        }

        /* Calculate the remained data_size  */
        total_size = total_size - sent_size;
        if (total_size > MQTTD_MAX_PACKET_SIZE)
        {
            msg_size = MQTTD_MAX_PACKET_SIZE - topic_size;
        }
        else
        {
            msg_size = total_size - topic_size;
        }
        sent_size = msg_size - MQTTD_MSG_HEADER_SIZE;
        pkt_idx++;
    } while (pkt_idx <= pkt_count);
    return MW_E_OK;
}

/* FUNCTION NAME: _mqttd_incoming_publish_cb
 * PURPOSE:
 *      MQTTD publish header callback function
 *
 * INPUT:
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *
 */
static void _mqttd_incoming_publish_cb(void *arg, const char *topic, u32_t tot_len)
{
    MQTTD_CTRL_T *ptr_mqttd = (MQTTD_CTRL_T *)arg;
    mqttd_debug_pkt("Incoming topic is \"%s\"", topic);
    osapi_memset(ptr_mqttd->pub_in_topic, 0, MQTTD_MAX_TOPIC_SIZE);
    osapi_strncpy(ptr_mqttd->pub_in_topic, topic, (MQTTD_MAX_TOPIC_SIZE-1));
}
#if 0
/* FUNCTION NAME: _mqttd_incoming_data_cb
 * PURPOSE:
 *      MQTTD publish data payload callback function
 *
 * INPUT:
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *
 */
static void _mqttd_incoming_data_cb(void *arg, const u8_t *data, u16_t len, u8_t flags)
{
    MQTTD_CTRL_T *ptr_mqttd = (MQTTD_CTRL_T *)arg;
    UI16_T idata = 0;
    UI16_T cldb_idx;
    C8_T new_cldbId[MQTTD_MAX_TOPIC_SIZE] = {0};
    osapi_snprintf(new_cldbId, MQTTD_MAX_TOPIC_SIZE, "%s/new/%s", MQTTD_TOPIC_CLOUD_PREFIX, ptr_mqttd->client_id);

    mqttd_debug_pkt("Incoming data length: %d", len);

    /* cmw/connected */
    if (0 == osapi_strcmp(ptr_mqttd->pub_in_topic, MQTTD_CLOUD_CONNECTED))
    {
        mqttd_debug_pkt("Incoming data: %s", data);
        mqttd_debug("cloud is connected");
    }
    /* cmw/new/<clientID> */
    else if (0 == osapi_strcmp(ptr_mqttd->pub_in_topic, new_cldbId))
    {
        if (len == sizeof(UI8_T))
        {
            idata = *data;
        }
        else
        {
            //idata = *((UI16_T *)data);
            idata = ((*(data+1) << 8) | (*data));
        }
        mqttd_debug_pkt("Get new cloud ID: %d", idata);

        if (ptr_mqttd->cldb_id != idata)
        {
            /* connect to new broker */
            ptr_mqttd->cldb_id = idata;
            mqttd_debug("mqttd switch id is \"%d\"", ptr_mqttd->cldb_id);
            ptr_mqttd->state = MQTTD_STATE_INITING;
        }
    }
    /* mwcloud/db */
    else if (0 == osapi_strcmp(ptr_mqttd->pub_in_topic, MQTTD_CLOUD_TODB))
    {
        mqttd_debug("receive cloud DB request");
        /* Send the data to internal DB */
        _mqttd_db_proxy(ptr_mqttd, data, len);
    }
    /* cmw/cgi/<cldb_ID> */
    else if (1 == sscanf(ptr_mqttd->pub_in_topic, MQTTD_CLOUD_CGI "/%hu", &cldb_idx))
    {
        if (ptr_mqttd->cldb_id == cldb_idx)
        {
            mqttd_debug("receive cloud CGI request");
            /* Call CGI function */
            _mqttd_cgi_proxy(ptr_mqttd, data, len);
        }
    }
    /* cmw/will */
    else if (0 == osapi_strcmp(ptr_mqttd->pub_in_topic, MQTTD_CLOUD_WILL))
    {
        mqttd_debug("receive cloud disconnected");
        ptr_mqttd->state = MQTTD_STATE_DISCONNECTED;
        mqttd_debug_pkt("Incoming data: %s", data);
    }
     /* Do nothing */
    else {}
}
#else
/* FUNCTION NAME: _mqttd_incoming_data_cb
 * PURPOSE:
 *      MQTTD publish data payload callback function
 *
 * INPUT:
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *
 */
static void _mqttd_incoming_data_cb(void *arg, const u8_t *data, u16_t len, u8_t flags)
{
    MQTTD_CTRL_T *ptr_mqttd = (MQTTD_CTRL_T *)arg;
    UI16_T idata = 0;
    C8_T new_topic[MQTTD_MAX_TOPIC_SIZE] = {0};
    osapi_snprintf(new_topic, MQTTD_MAX_TOPIC_SIZE, "%s/tx", ptr_mqttd->topic_prefix);

    mqttd_debug_pkt("Incoming data length: %d", len);

    /* tx */
    if (0 == osapi_strcmp(ptr_mqttd->pub_in_topic, new_topic))
    {
        mqttd_debug_pkt("Incoming data: %s", data);
        // Allocate memory for decoded data
        unsigned char *decoded_data = NULL;
        if (osapi_malloc(len, MQTTD_TASK_NAME, &decoded_data) != MW_E_OK)
        {
            mqttd_debug("Failed to allocate memory for decoded data.");
            return;
        }

        // Decode the data into the allocated memory
        mqttd_rc4_decrypt((unsigned char *)data, len, MQTTD_RC4_KEY, decoded_data);

        // Use the decoded data
        mqttd_debug_pkt("Decoded data: %s", decoded_data);

        // Parse the JSON data using cJSON
        cJSON *json_obj = cJSON_Parse((const char *)decoded_data);
        if (json_obj == NULL)
        {
            mqttd_debug("Failed to parse JSON data.");
            osapi_free(decoded_data);
            return;
        }

        // Check the type field in the JSON data
        cJSON *type_obj = cJSON_GetObjectItemCaseSensitive(json_obj, "type");
        if (cJSON_IsString(type_obj) && (type_obj->valuestring != NULL))
        {
            const char *type_str = type_obj->valuestring;
            mqttd_debug("Type in JSON data: %s", type_str);

            // Handle different types
            if (osapi_strcmp(type_str, "capability") == 0)
            {
                // Handle the desired type
                mqttd_debug("Handling capability type.");
            }
            else if (osapi_strcmp(type_str, "rules") == 0)
            {
                mqttd_debug("Handling rules type.");
            }
            else if (osapi_strcmp(type_str, "getConfig") == 0)
            {
                mqttd_debug("Handling getConfig type.");
            }
            else if (osapi_strcmp(type_str, "setConfig") == 0)
            {
                mqttd_debug("Handling setConfig type.");
            }
            else if (osapi_strcmp(type_str, "reset") == 0)
            {
                mqttd_debug("Handling reset type.");
            }
            else if (osapi_strcmp(type_str, "reboot") == 0)
            {
                mqttd_debug("Handling reboot type.");
            }
            else if (osapi_strcmp(type_str, "rebootPort") == 0)
            {
                mqttd_debug("Handling rebootPort type.");
            }
            else if (osapi_strcmp(type_str, "check") == 0)
            {
                mqttd_debug("Handling check type.");
            }
            else if (osapi_strcmp(type_str, "tunnel") == 0)
            {
                mqttd_debug("Handling tunnel type.");
            }
            else if (osapi_strcmp(type_str, "update") == 0)
            {
                mqttd_debug("Handling update type.");
            }
            else if (osapi_strcmp(type_str, "logs") == 0)
            {
                mqttd_debug("Handling logs type.");
            }
            else if (osapi_strcmp(type_str, "bind") == 0)
            {
                mqttd_debug("Handling bind type.");
            }
            else
            {
                mqttd_debug("Unhandled type: %s", type_str);
            }
        }
        else
        {
            mqttd_debug("Type field not found in JSON data.");
        }
        // Clean up
        cJSON_Delete(json_obj);

        osapi_free(decoded_data);
    }
     /* Do nothing */
    else 
    {
        mqttd_debug("No valid topic found, doing nothing.");
    }
}

#endif



#if 0
/* FUNCTION NAME:  _mqttd_subscribe_cb
 * PURPOSE:
 *      MQTTD SUBSCRIBE callback function
 *
 * INPUT:
 *      arg     --  The pointer of MQTTD ctrl structure
 *      err     --  The subscribe process error status
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 *
 */
static void _mqttd_subscribe_cb(void *arg, err_t err)
{
    MQTTD_CTRL_T *ptr_mqttd = (MQTTD_CTRL_T *)arg;
    C8_T reg_new[32] = {0};
    C8_T reg_msg[36] = {0};
    mqttd_debug("mqtt subscribe error state is (%d)\n", (UI8_T)err);

    if (err == ERR_OK)
    {
        /* If SUBACK received, then PUBLISH the device client-id to get the cloud DB ID */
        osapi_snprintf(reg_new, sizeof(reg_new), "%s/%s", MQTTD_TOPIC_NEW, ptr_mqttd->client_id);
        osapi_snprintf(reg_msg, sizeof(reg_msg), MQTTD_CLIENT_CONNECT_MSG_FMT, PLAT_MAX_PORT_NUM, MQTTD_MSG_VER);
        mqtt_publish(ptr_mqttd->ptr_client, reg_new, (const void *)reg_msg, osapi_strlen(reg_msg), MQTTD_REQUEST_QOS, MQTTD_REQUEST_RETAIN, _mqttd_publish_cb, (void *)ptr_mqttd);
        ptr_mqttd->state = MQTTD_STATE_SUBACK;
    }
    else
    {
        /* If SUBSCRIBE failed, then try to re-subscribe again */
        osapi_printf("\n MQTT SUBSCRIBE failed (%d)\n", (UI8_T)err);
        ptr_mqttd->state = MQTTD_STATE_CONNECTED;
    }
}
#else
/* FUNCTION NAME:  _mqttd_subscribe_cb
 * PURPOSE:
 *      MQTTD SUBSCRIBE callback function
 *
 * INPUT:
 *      arg     --  The pointer of MQTTD ctrl structure
 *      err     --  The subscribe process error status
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 *
 */
static void _mqttd_subscribe_cb(void *arg, err_t err)
{
    MQTTD_CTRL_T *ptr_mqttd = (MQTTD_CTRL_T *)arg;

    mqttd_debug("mqtt subscribe error state is (%d)\n", (UI8_T)err);

    if (err == ERR_OK)
    {
        /* If SUBACK received, then PUBLISH online event */
        char topic[128];
        osapi_snprintf(topic, sizeof(topic), "%s/event", ptr_mqttd->topic_prefix);
        cJSON *root = cJSON_CreateObject();
        cJSON *data = cJSON_CreateObject();

        if (root == NULL || data == NULL) {
            mqttd_debug("Failed to create JSON objects\n");
            if (root != NULL) cJSON_Delete(root);
            if (data != NULL) cJSON_Delete(data);
            return;
        }

	    cJSON_AddStringToObject(root, "type", "online");
	    cJSON_AddItemToObject(root, "data", data);

	    cJSON_AddStringToObject(data, "swid", ptr_mqttd->device_id);
	    cJSON_AddNumberToObject(data, "runtime", 100);
	    cJSON_AddStringToObject(data, "version", "1.0.0");
	    cJSON_AddStringToObject(data, "product_name", "G50");
	    cJSON_AddStringToObject(data, "firmware", "m1-os");
	    cJSON_AddStringToObject(data, "sn", ptr_mqttd->sn);
	    cJSON_AddStringToObject(data, "type", "L2");
	    cJSON_AddStringToObject(data, "mac", ptr_mqttd->mac);

	    char *original_payload = cJSON_PrintUnformatted(root);
	    cJSON_Delete(root);

	    if (original_payload == NULL) {
	        printf("Failed to print JSON\n");
	        return;
	    }

		int original_payloadlen = strlen(original_payload);
		unsigned char encoded_payload[256]; // Ensure this is large enough for your payload
		// Encrypt the payload using RC4
    	mqttd_rc4_encrypt((unsigned char *)original_payload, original_payloadlen, MQTTD_RC4_KEY, encoded_payload);
        mqtt_publish(ptr_mqttd->ptr_client, topic, (const void *)encoded_payload, original_payloadlen, MQTTD_REQUEST_QOS, MQTTD_REQUEST_RETAIN, _mqttd_publish_cb, (void *)ptr_mqttd);
		free(original_payload); // Free the JSON payload
		
		osapi_printf("\n MQTT subscribe tx topic done.\n");

		
		ptr_mqttd->state = MQTTD_STATE_SUBACK;
    }
    else
    {
        /* If SUBSCRIBE failed, then try to re-subscribe again */
        osapi_printf("\n MQTT SUBSCRIBE failed (%d)\n", (UI8_T)err);
        ptr_mqttd->state = MQTTD_STATE_CONNECTED;
    }
}
#endif

#if 0
/* FUNCTION NAME:  _mqttd_send_subscribe
 * PURPOSE:
 *      MQTTD send SUBSCRIBE ctrl packet
 *
 * INPUT:
 *      client  --  The pointer of client handle
 *      arg     --  The pointer of MQTTD ctrl structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 *
 */
static void _mqttd_send_subscribe(mqtt_client_t *client, void *arg)
{
    err_t err;
    MQTTD_CTRL_T *ptr_mqttd = (MQTTD_CTRL_T *)arg;
    mqttd_debug("mqtt send subscribe\n");
    // Send subscribe string
    err = mqtt_subscribe(client, MQTTD_CLOUD_TODB, MQTTD_REQUEST_QOS, _mqttd_subscribe_cb, arg);
    if (err == ERR_OK)
    {
        ptr_mqttd->state = MQTTD_STATE_SUBSCRIBE;
    }

    err = mqtt_subscribe(client, MQTTD_SUB_CLOUD_FILTER, MQTTD_REQUEST_QOS, _mqttd_subscribe_cb, arg);
    if (err == ERR_OK)
    {
        ptr_mqttd->state = MQTTD_STATE_SUBSCRIBE;
    }
}
#else
/* FUNCTION NAME:  _mqttd_send_subscribe
 * PURPOSE:
 *      MQTTD send SUBSCRIBE ctrl packet
 *
 * INPUT:
 *      client  --  The pointer of client handle
 *      arg     --  The pointer of MQTTD ctrl structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 *
 */
static void _mqttd_send_subscribe(mqtt_client_t *client, void *arg)
{
    err_t err;
    MQTTD_CTRL_T *ptr_mqttd = (MQTTD_CTRL_T *)arg;
    mqttd_debug("mqtt send subscribe\n");
    char topic[128];
    osapi_snprintf(topic, sizeof(topic), "%s/tx", ptr_mqttd->topic_prefix);

    // Send subscribe tx
    err = mqtt_subscribe(client, topic, MQTTD_REQUEST_QOS, _mqttd_subscribe_cb, arg);
    if (err == ERR_OK)
    {
        ptr_mqttd->state = MQTTD_STATE_SUBSCRIBE;
    }

}
#endif
/* FUNCTION NAME:  _mqttd_connection_cb
 * PURPOSE:
 *      MQTTD connection callback function
 *
 * INPUT:
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *
 * NOTES:
 *  This callback function may be called when MQTT_CONNECTED or TCP_DISCONNECTED
 */
static void _mqttd_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status)
{
    MQTTD_CTRL_T *ptr_mqttd = (MQTTD_CTRL_T *)arg;

    osapi_printf("mqtt connection state is \"%s(%d)\"\n", (status == MQTT_CONNECT_ACCEPTED)? "CONNECTED" : "DISCONNECTED", (UI16_T)status);

    if (status != MQTT_CONNECT_ACCEPTED)
    {
        // If callback by mqtt_close, then free the client
        if (ptr_mqttd->state < MQTTD_STATE_DISCONNECTED)
        {
            ptr_mqttd->state = MQTTD_STATE_DISCONNECTED;
            ptr_mqttd->reconnect = TRUE;
        }
        return;
    }

    // Set publish callback functions
    mqtt_set_inpub_callback(mqttd.ptr_client, _mqttd_incoming_publish_cb, _mqttd_incoming_data_cb, (void *)&mqttd);
    ptr_mqttd->state = MQTTD_STATE_CONNECTED;
}

#if LWIP_DNS
/* FUNCTION NAME: _mqttd_dns_found
 * PURPOSE:
 *      A callback function from dns
 *
 * INPUT:
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
void _mqttd_dns_found(const char *name, const ip_addr_t *ipaddr, void *callback_arg)
{
    MQTTD_CTRL_T *ptr_mqttd = (MQTTD_CTRL_T *)callback_arg;
    MW_ERROR_NO_T rc;

    if (ipaddr == IPADDR_ANY)
    {
        osapi_printf("server \"%s\" lookup failed, use default server\n", name);
        ip_addr_copy(ptr_mqttd->server_ip, mqttd_server_ip);
    }
    rc = _mqttd_client_connect(ptr_mqttd);
    if (rc == MW_E_NO_MEMORY)
    {
        osapi_printf("Failed to connect to mqtt server due to no memory, close mqttd!\n");
        ptr_mqttd->state = MQTTD_STATE_SHUTDOWN;
        ptr_mqttd->reconnect = FALSE;
    }
    else if (rc != MW_E_OK)
    {
        osapi_printf("Failed to connect to mqtt server: %s\n", name);
        ptr_mqttd->state = MQTTD_STATE_DISCONNECTED;
        ptr_mqttd->reconnect = TRUE;
    }
    else
    { /* Do nothing */}
}
#endif

/* FUNCTION NAME: _mqttd_lookup_server
 * PURPOSE:
 *      Lookup the remote server IP address
 *
 * INPUT:
 *      ptr_mqttd  --  the pointer of the mqttd ctrl structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      MW_E_OK
 *      MW_E_NO_MEMORY
 *      MW_E_BAD_PARAMETER
 *
 * NOTES:
 *      None
 */
static MW_ERROR_NO_T _mqttd_lookup_server(MQTTD_CTRL_T *ptr_mqttd)
{
    I8_T err = ERR_OK;
    ip4_addr_t *ptr_server = NULL;

    if (ptr_mqttd == NULL)
    {
        return MW_E_BAD_PARAMETER;
    }
    ptr_server = ip_2_ip4(&(ptr_mqttd->server_ip));
    if (ptr_server->addr != IPADDR_ANY)
    {
        /* server ip set by console */
        mqttd_debug("Connect to original MQTT remote server");
        return _mqttd_client_connect(ptr_mqttd);
    }

#if LWIP_DNS
    err = dns_gethostbyname((const char *)cloud_hostname, &(ptr_mqttd->server_ip), _mqttd_dns_found, (void *)ptr_mqttd);
#else
    ip_addr_copy(ptr_mqttd->server_ip, mqttd_server_ip);
#endif
	ptr_mqttd->port = MQTT_SRV_PORT;
    if (err == ERR_OK)
    {
        return _mqttd_client_connect(ptr_mqttd);
    }
    else if (err != ERR_INPROGRESS)
    {
        // failed due to memory or argument error
        osapi_printf("Cannot use DNS:(error %d), use MQTTD default remote server\n", (int)err);
        ip_addr_copy(ptr_mqttd->server_ip, mqttd_server_ip);
        return _mqttd_client_connect(ptr_mqttd);
    }
    return MW_E_OK;
}
/* FUNCTION NAME:  _mqttd_client_connect
 * PURPOSE:
 *      Generate the Client ID and connect to the remote MQTT server.
 *
 * INPUT:
 *      ptr_mqttd  --  the pointer of the mqttd ctrl structure
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      MW_E_OK
 *      MW_E_NO_MEMORY
 *
 * NOTES:
 *      None
 */
static MW_ERROR_NO_T _mqttd_client_connect(MQTTD_CTRL_T *ptr_mqttd)
{
    I8_T err = ERR_OK;
    C8_T username[MQTTD_MAX_CLIENT_ID_SIZE] = MQTTD_USERNAME;
    C8_T password[MQTTD_MAX_CLIENT_ID_SIZE] = MQTTD_PASSWD;
    //C8_T will_topic[MQTTD_MAX_TOPIC_SIZE] = MQTTD_WILL_TOPIC;
    struct mqtt_connect_client_info_t client_info;
    UI16_T portnum = MQTT_SRV_PORT;

    if (ptr_mqttd->ptr_client != NULL)
    {
        if (TRUE == mqtt_client_is_connected(ptr_mqttd->ptr_client))
        {
            return MW_E_OK;
        }
    }

    /* Generate the client information */
    client_info = (struct mqtt_connect_client_info_t){
        (const char *)ptr_mqttd->client_id,  /* Client identifier, must be set by caller */
        username,                 /* User name, set to NULL if not used */
        password,                 /* Password, set to NULL if not used */
        MQTTD_KEEP_ALIVE,         /* keep alive time in seconds, 0 to disable keep alive functionality*/
        (const char *)NULL, /* will topic, set to NULL if will is not to be used,
                                     will_msg, will_qos and will retain are then ignored */
        (const char *)ptr_mqttd->client_id,  /* will_msg, see will_topic */
        MQTTD_WILL_QOS,           /* will_qos, see will_topic */
        MQTTD_WILL_RETAIN         /* will_retain, see will_topic */
    };

    mqttd_debug("MQTTD create a new client (%p) try to connect to %s", ptr_mqttd->ptr_client, ipaddr_ntoa(&ptr_mqttd->server_ip));
    if (ptr_mqttd->ptr_client == NULL)
    {
        ptr_mqttd->ptr_client = mqtt_client_new();
        if (ptr_mqttd->ptr_client == NULL)
        {
            osapi_printf("\nconnect_mqtt: create new mqtt client failed.\n");
            return MW_E_NO_MEMORY;
        }
    }

    // connect to MQTT server
    err = mqtt_client_connect(ptr_mqttd->ptr_client, (const ip_addr_t *)&(ptr_mqttd->server_ip), portnum,
            _mqttd_connection_cb, (void *)ptr_mqttd, (const struct mqtt_connect_client_info_t *)&client_info);
    if (ERR_OK != err)
    {
        osapi_printf("\nconnect_mqtt: connect to remote mqtt server failed: %d.\n", err);
        if (ERR_MEM == err)
        {
            return MW_E_NO_MEMORY;
        }
        else
        {
            return MW_E_OP_INVALID;
        }
    }

    return MW_E_OK;
}

/* FUNCTION NAME:  _mqttd_client_disconnect
 * PURPOSE:
 *      Disconnect to the remote MQTT server.
 *
 * INPUT:
 *      ptr_mqttclient   --  the pointer of the client
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
static void _mqttd_client_disconnect(mqtt_client_t* ptr_mqttclient)
{
    if (ptr_mqttclient != NULL)
    {
        // Disconnect MQTT
        mqttd_debug("MQTTD client disconnect.");
        mqtt_disconnect(ptr_mqttclient);
    }
}

/* FUNCTION NAME:  _mqttd_reconnect_tmr
 * PURPOSE:
 *      The timer to do the reconnect process
 *
 * INPUT:
 *      ptr_xTimer
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */

static void _mqttd_reconnect_tmr(timehandle_t ptr_xTimer)
{
    if (mqttd.state == MQTTD_STATE_SHUTDOWN)
    {
        if (NULL != ptr_mqttdmain)
        {
            mqttd_debug("Close MQTTD main task.");
            ptr_mqttdmain = NULL;
        }
        if (TRUE == mqttd.reconnect)
        {
            osapi_printf("Reconnect to remote server: %s\n", ipaddr_ntoa(&mqttd.server_ip));
            if (MW_E_OK != mqttd_init((void *)&(mqttd.server_ip)))
            {
                osapi_printf("Failed to Reconnect to remote server: Operation Stopped.\n");
            }
        }
    }
}
# if 0
static void _mqttd_main(void *arg)
{
    MW_ERROR_NO_T rc = MW_E_NOT_INITED;
    UI8_T netif_num = 0;
    struct netif *xNetIf = NULL;

    /* Waiting until DB and Netif are ready */
    do
    {
        rc = dbapi_dbisReady();
    }while(rc != MW_E_OK);

    /* Initialize client ID */
    (void)_mqttd_gen_client_id(&mqttd);

    do{
        netif_num = netif_num_get();
        xNetIf = netif_get_by_index(netif_num);
        osapi_delay(MQTTD_MUX_LOCK_TIME);
    } while (xNetIf == NULL);

    mqttd_debug("MQTTD use NetIf num:%d, name:%s, mac:[%02x:%02x:%02x:%02x:%02x:%02x]",
        xNetIf->num, xNetIf->name,
        xNetIf->hwaddr[0],
        xNetIf->hwaddr[1],
        xNetIf->hwaddr[2],
        xNetIf->hwaddr[3],
        xNetIf->hwaddr[4],
        xNetIf->hwaddr[5]);

    do{
        /* Connect to MQTT */
        rc = _mqttd_lookup_server(&mqttd);
        if (rc == MW_E_NO_MEMORY)
        {
            osapi_printf("Failed to connect to mqtt server due to no memory, close mqttd!\n");
            mqttd.state = MQTTD_STATE_DISCONNECTED;
            break;
        }
        else if (rc != MW_E_OK)
        {
            mqttd_debug("Failed to connect to mqtt server.\n");
            mqttd.reconnect = TRUE;
            mqttd.state = MQTTD_STATE_DISCONNECTED;
            break;
        }
        else
        {
            if(MW_E_OK != osapi_timerStart(ptr_mqttd_time))
            {
                osapi_printf("Failed to start MQTTD timer. MQTTD Stopped.\n");
                mqttd.state = MQTTD_STATE_DISCONNECTED;
                break;
            }
        }
    }while(0);

    while(mqttd.state < MQTTD_STATE_DISCONNECTED)
    {
        switch (mqttd.state)
        {
            case MQTTD_STATE_CONNECTED:
            {
                // Set publish callback functions
                _mqttd_send_subscribe(mqttd.ptr_client, (void *)&mqttd);
                break;
            }
            case MQTTD_STATE_INITING:
            {
                if (MW_E_OK == _mqttd_subscribe_db(&mqttd))
                {
                    if (mqttd.state != MQTTD_STATE_DISCONNECTED)
                    {
                        mqttd.state = MQTTD_STATE_RUN;
                    }
                }
                break;
            }
            case MQTTD_STATE_RUN:
            {
                /* Waiting for DB send notification */
                _mqttd_listen_db(&mqttd);
                break;
            }
            default:
            {
                osapi_delay(MQTTD_MUX_LOCK_TIME);
                //mqttd_debug("MQTTD process state %d", mqttd.state);
                break;
            }
        }
    }

    _mqttd_deinit();
}
#endif
/* EXPORTED SUBPROGRAM BODIES
 */
/* FUNCTION NAME: mqttd_init
 * PURPOSE:
 *      Initialize MQTT client daemon
 *
 * INPUT:
 *      arg  --  the remote server IP
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      MW_E_OK
 *      MW_E_NOT_INITED
 *
 * NOTES:
 *      If connect to default remote MQTT server,  then set arg to NULL.
 */
MW_ERROR_NO_T mqttd_init(void *arg)
{
    MW_ERROR_NO_T rc = MW_E_OK;

    if (ptr_mqttdmain != NULL)
    {
        mqttd_debug("MQTTD %p already exist.",ptr_mqttdmain);
        return MW_E_ALREADY_INITED;
    }
    mqttd_debug("Create the MQTTD task.");

    /* mqttd control structure initialize */
    _mqttd_ctrl_init(&mqttd, (ip_addr_t *)arg);

    /* mqttd internal DB queue */
    rc = mqttd_queue_init();
    if (MW_E_OK != rc)
    {
        return MW_E_NOT_INITED;
    }

    /* mqttd remain message mutex */
    rc = osapi_mutexCreate(
            MQTTD_TASK_NAME,
            &ptr_mqttmutex);
    if (MW_E_OK != rc)
    {
        mqttd_debug("Failed to create remain message mutex");
        mqttd_queue_free();
        return MW_E_NOT_INITED;
    }
    mqttd_debug("Create the remain msg mutex %p",ptr_mqttmutex);

    /* Create timer */
    osapi_timerCreate(
            MQTTD_TIMER_NAME,
            _mqttd_tmr,
            TRUE,
            MQTTD_TIMER_PERIOD,
            NULL,
            &ptr_mqttd_time);
    if(NULL == ptr_mqttd_time)
    {
        mqttd_debug("Failed to create MQTTD timer.");
        mqttd_queue_free();
        osapi_mutexDelete(ptr_mqttmutex);
        return MW_E_NOT_INITED;
    }

    /* mqttd main process */
    rc = osapi_processCreate(
            MQTTD_TASK_NAME,
            MQTTD_STACK_SIZE,
            MQTTD_THREAD_PRI,
            _mqttd_main,
            (void *)&ptr_mqttdmain,
            &ptr_mqttdmain);

    if (MW_E_OK != rc)
    {
        mqttd_debug("Delete the remain message mutex and process mutex due to process create failed");
        mqttd_queue_free();
        osapi_mutexDelete(ptr_mqttmutex);
        osapi_timerDelete(ptr_mqttd_time);
        return MW_E_NOT_INITED;
    }
    mqttd_enable = TRUE;

    osapi_printf("\nMQTTD create the process: %p\n",ptr_mqttdmain);
    return MW_E_OK;
}

/* FUNCTION NAME: _mqttd_deinit
 * PURPOSE:
 *      Deinitialize MQTT client daemon
 *
 * INPUT:
 *      None
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      MW_E_OK
 *
 * NOTES:
 *      None
 */
MW_ERROR_NO_T _mqttd_deinit(void)
{
    if (ptr_mqttdmain == NULL)
    {
        if (MQTTD_STATE_SHUTDOWN != mqttd.state)
        {
            mqttd.state = MQTTD_STATE_SHUTDOWN;
        }
        return MW_E_OK;
    }
    mqttd.state = MQTTD_STATE_SHUTDOWN;
    _mqttd_unsubscribe_db(&mqttd);
    _mqttd_client_disconnect(mqttd.ptr_client);
    _mqttd_ctrl_free(&mqttd);
    if(NULL != ptr_mqttd_time)
    {
        mqttd_debug("MQTTD free the timer.");
        if(MW_E_OK == osapi_timerActive(ptr_mqttd_time))
        {
            osapi_timerStop(ptr_mqttd_time);
        }
        osapi_timerDelete(ptr_mqttd_time);
        ptr_mqttd_time = NULL;
    }
    if(NULL != ptr_mqttmutex)
    {
        mqttd_debug("MQTTD free the remain message semaphore.");
        osapi_mutexDelete(ptr_mqttmutex);
        ptr_mqttmutex = NULL;
    }
    mqttd_queue_free();

    /* Create reconnect timer */
    if(NULL == ptr_mqttd_recon_time)
    {
        osapi_timerCreate(
                MQTTD_RECONNECT_TIMER_NAME,
                _mqttd_reconnect_tmr,
                FALSE,
                MQTTD_TIMER_RECONNECT_PERIOD,
                NULL, &ptr_mqttd_recon_time);
    }
    if(NULL != ptr_mqttd_recon_time)
    {
        mqttd_reconnect();
    }
    else
    {
        osapi_printf("Failed to create MQTTD Reconnect timer: No memory.\n");
    }
    osapi_processDelete(ptr_mqttdmain);
    ptr_mqttdmain = NULL;
    return MW_E_OK;
}

#if 0
/* FUNCTION NAME: mqttd_dump_topic
 * PURPOSE:
 *      Dump all mqtt supported topics
 *
 * INPUT:
 *      None
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      For debug using only
 */


void mqttd_dump_topic(void)
{
    C8_T topic[MQTTD_MAX_TOPIC_SIZE] = {0};
    UI8_T t_idx = 0, f_idx = 0, f_count;
    UI16_T e_idx = 0, length;

    osapi_printf("\nMQTTD supported DB Topics:\n");
    while (t_idx < TABLES_LAST)
    {
        if (MW_E_OK == dbapi_getFieldsNum(t_idx, &f_count))
        {
            for (f_idx = 0; f_idx < f_count; f_idx++)
            {
                length = _mqttd_db_topic_set(&mqttd, M_CREATE, t_idx, f_idx, e_idx, topic, MQTTD_MAX_TOPIC_SIZE);
                osapi_printf("(%-3u/%-3u/%-5u): %-42s (len= %2d).\n", t_idx, f_idx, e_idx, topic, length);
            }
        }
        t_idx++;
    }
}

#else
void mqttd_dump_topic(void)
{
    osapi_printf("\nMQTTD supported DB Topics\n");
}

#endif

/* FUNCTION NAME: mqttd_debug_enable
 * PURPOSE:
 *      To enable or disable to print debug message
 *
 * INPUT:
 *      level    --  The debugging level
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
void mqttd_debug_enable(UI8_T level)
{
    if (level == mqttd_debug_level)
    {
        return;
    }

    if (level < MQTTD_DEBUG_LAST)
    {
        mqttd_debug_level = level;
    }
}

/* FUNCTION NAME: mqttd_show_state
 * PURPOSE:
 *      To show the mqttd status
 *
 * INPUT:
 *      None
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
void mqttd_show_state(void)
{
    if (MQTTD_STATE_RUN > mqttd.state)
    {
        osapi_printf("\nMQTTD state 	  : Connecting(%d)\n", mqttd.state);
        return;
    }
    else if (MQTTD_STATE_RUN < mqttd.state)
    {
        osapi_printf("\nMQTTD state 	  : Stopped(%d)\n", mqttd.state);
        return;
    }
    else
    {
        osapi_printf("\nMQTTD state 	  : Running(%d)\n", mqttd.state);
    }
    osapi_printf("MQTT remote server: %s\n", ipaddr_ntoa(&mqttd.server_ip));
    osapi_printf("MQTTD Client ID   : %s\n", mqttd.client_id);
    osapi_printf("MQTTD cloud ID    : %d\n", mqttd.cldb_id);
}

/* FUNCTION NAME: mqttd_reconnect
 * PURPOSE:
 *      To active the timer to do the shutdown and restart mqttd main task
 *
 * INPUT:
 *      None
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
void mqttd_reconnect(void)
{
    if(MW_E_OK == osapi_timerActive(ptr_mqttd_recon_time))
    {
        mqttd_debug(" MQTTD Reconnect timer is active .\n");
        return;
    }
    if(MW_E_OK != osapi_timerStart(ptr_mqttd_recon_time))
    {
        mqttd_debug("!Error: Failed to start MQTTD Reconnect timer: Operation Stopped.\n");
        return;
    }
}

/* FUNCTION NAME: mqttd_shutdown
 * PURPOSE:
 *      To shutdown the MQTTD
 *
 * INPUT:
 *      None
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      None
 *
 * NOTES:
 *      None
 */
void mqttd_shutdown(void)
{
    if (ptr_mqttdmain == NULL)
    {
        mqttd_debug("MQTTD stopped.");
        if (MQTTD_STATE_SHUTDOWN != mqttd.state)
        {
            mqttd.state = MQTTD_STATE_SHUTDOWN;
        }
        return;
    }
    mqttd_debug("Shutdown the MQTTD task.");

    mqttd.state = MQTTD_STATE_SHUTDOWN;
    mqttd.reconnect = FALSE;
    mqttd_enable = FALSE;
}

/* FUNCTION NAME: mqttd_get_state
 * PURPOSE:
 *      To get the MQTTD status
 *
 * INPUT:
 *      None
 *
 * OUTPUT:
 *      None
 *
 * RETURN:
 *      BOOL_T
 *
 * NOTES:
 *      None
 */
UI8_T
mqttd_get_state(
    void)
{
    return mqttd_enable;
}


static void _mqttd_gen_client_id(MQTTD_CTRL_T *ptr_mqttd)
{
    C8_T ident[] = "sw:hongrui:hongrui:2024";
    C8_T device_str[128] = {0};
    C8_T device_md5[16] = {0};
	
    //C8_T device_md5_str[33] = {0};
	C8_T manufacturer[] = "hongrui";
	C8_T device_type[] = "sw";
	C8_T device_id_str[64] = {0};
	int i;
    // Get SN and MAC address
    // TODO: Implement functions to get actual SN and MAC
    osapi_strncpy(ptr_mqttd->sn, "C171Z1YM000000", MQTTD_MAX_SN_SIZE-1);
    osapi_strncpy(ptr_mqttd->mac, "1C:2A:A3:00:00:2F", MQTTD_MAX_MAC_SIZE-1);
    
    // Combine SN, MAC and IDENT
    osapi_snprintf(device_str, sizeof(device_str), "%s%s%s", ptr_mqttd->sn, ptr_mqttd->mac, ident);
    
    // Calculate MD5 of combined string
    md5(device_str, osapi_strlen(device_str), device_md5);

    for (i = 0; i < 16; i++) {
        osapi_snprintf(&ptr_mqttd->device_id[i * 2], 3, "%02x", (unsigned char)device_md5[i]);
    }
    
    // Copy MD5 result to client_id
    osapi_snprintf(ptr_mqttd->topic_prefix, MQTTD_MAX_TOPIC_PREFIX_SIZE - 1, "%s/%s/%s", manufacturer, device_type, ptr_mqttd->device_id);
    osapi_snprintf(ptr_mqttd->client_id, MQTTD_MAX_CLIENT_ID_SIZE - 1, "%s:%s:%s", manufacturer, device_type, ptr_mqttd->device_id);
    ptr_mqttd->topic_prefix[MQTTD_MAX_TOPIC_PREFIX_SIZE - 1] = '\0';
	ptr_mqttd->client_id[MQTTD_MAX_CLIENT_ID_SIZE - 1] = '\0';
    osapi_printf("Client ID: %s\n", ptr_mqttd->client_id);
	osapi_printf("Topic Prefix: %s\n", ptr_mqttd->topic_prefix);
    return;
}


static void _mqttd_main(void *arg)
{
    MW_ERROR_NO_T rc = MW_E_NOT_INITED;
    UI8_T netif_num = 0;
    struct netif *xNetIf = NULL;

    /* Waiting until DB and Netif are ready */
    do
    {
        rc = dbapi_dbisReady();
    }while(rc != MW_E_OK);

    /* Initialize client ID */
    (void)_mqttd_gen_client_id(&mqttd);

    do{
        netif_num = netif_num_get();
        xNetIf = netif_get_by_index(netif_num);
        osapi_delay(MQTTD_MUX_LOCK_TIME);
    } while (xNetIf == NULL);

    mqttd_debug("MQTTD use NetIf num:%d, name:%s, mac:[%02x:%02x:%02x:%02x:%02x:%02x]",
        xNetIf->num, xNetIf->name,
        xNetIf->hwaddr[0],
        xNetIf->hwaddr[1],
        xNetIf->hwaddr[2],
        xNetIf->hwaddr[3],
        xNetIf->hwaddr[4],
        xNetIf->hwaddr[5]);

    do{
        /* Connect to MQTT */
        rc = _mqttd_lookup_server(&mqttd);
        if (rc == MW_E_NO_MEMORY)
        {
            osapi_printf("Failed to connect to mqtt server due to no memory, close mqttd!\n");
            mqttd.state = MQTTD_STATE_DISCONNECTED;
            break;
        }
        else if (rc != MW_E_OK)
        {
            mqttd_debug("Failed to connect to mqtt server.\n");
            mqttd.reconnect = TRUE;
            mqttd.state = MQTTD_STATE_DISCONNECTED;
            break;
        }
        else
        {
            if(MW_E_OK != osapi_timerStart(ptr_mqttd_time))
            {
                osapi_printf("Failed to start MQTTD timer. MQTTD Stopped.\n");
                mqttd.state = MQTTD_STATE_DISCONNECTED;
                break;
            }
        }
    }while(0);

    while(mqttd.state < MQTTD_STATE_DISCONNECTED)
    {
        switch (mqttd.state)
        {
            case MQTTD_STATE_CONNECTED:
            {
                // Set publish callback functions
                _mqttd_send_subscribe(mqttd.ptr_client, (void *)&mqttd);//TO MQTT SERVER
                break;
            }
            case MQTTD_STATE_INITING:
            {
                if (MW_E_OK == _mqttd_subscribe_db(&mqttd)) //TO DB PROXY
                {
                    if (mqttd.state != MQTTD_STATE_DISCONNECTED)
                    {
                        mqttd.state = MQTTD_STATE_RUN;
                    }
                }
                break;
            }
            case MQTTD_STATE_RUN:
            {
                /* Waiting for DB send notification */
                _mqttd_listen_db(&mqttd);
                break;
            }
            default:
            {
                osapi_delay(MQTTD_MUX_LOCK_TIME);
                //mqttd_debug("MQTTD process state %d", mqttd.state);
                break;
            }
        }
    }

    _mqttd_deinit();
}


