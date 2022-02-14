using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Storage.Queues;
using Azure.Storage;
using Azure;

namespace Awara.Retail.Adept.UploadFiles
{
    public static class QueueRequest
    {
        [FunctionName("QueueRequest")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function QueueRequest processed a request.");
            IActionResult result;

            try
            {
                string command = req.Query["command"];
                string key_dir = req.Query["key-dir"];
                string message = req.Query["message"];
                string method = req.Method;

                log.LogInformation(string.Format("method: {0}", method));

                if (string.IsNullOrEmpty(key_dir))
                    throw new Exception("Parameter key-dir is empty.");

                if (method == "POST")
                {
                    if (string.IsNullOrEmpty(command))
                        throw new Exception("Parameter command is empty.");

                    if (command == "create")

                        createQueue(key_dir);

                    else if (command == "put")
                    {
                        if (string.IsNullOrEmpty(message))
                            throw new Exception("Parameter message is empty.");
                        putIntoQueue(key_dir, message);
                    }
                    else
                        throw new Exception(string.Format("Command:{0} not identified. ", command));

                    result = new OkObjectResult("OK");
                }
                else if (method == "GET")
                {
                    string messages = getFromQueue(key_dir);
                    log.LogInformation(messages);
                    result = new OkObjectResult(messages);
                }
                else
                    throw new Exception(string.Format("Using a command:{0} that does not correspond to the function interface.", method));
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);

                result = new BadRequestObjectResult(ex);
            }
            finally { }

            return result;
        }
        /// <summary>
        /// Создать очередь сообщений
        /// </summary>
        /// <param name="key_dir"></param>
        private static void createQueue(string key_dir)
        {
            QueueClient queue = prepareQueue(key_dir);
            queue.Create();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key_dir"></param>
        /// <param name="message"></param>
        private static void putIntoQueue(string key_dir, string message)
        {
            QueueClient queue = prepareQueue(key_dir);
            queue.SendMessage(message);
        }
        /// <summary>
        /// Получить сообщения из очереди
        /// </summary>
        /// <param name="key_dir">идентификатор очереди</param>
        /// <returns></returns>
        private static string getFromQueue(string key_dir)
        {
            string messages_json = "";
            int maxMessages = 5;
            string sMinutesHide = Utils.GetEnvironmentVariable("Queue_Minutes_Hide");
            string template_queue = @"{{""messages"":[{0}]}}";
            string template_message = @"{{""messageid"":""{0}"", 
                ""insertiontime"":""{1}"",
            	""expirationtime"":""{2}"",
            	""popreceipt"":""{3}"",
            	""timenextvisible"":""{4}"",
            	""dequeuecount"":""{5}"",
            	""content"":{6}
            }}";

            int minutesHide;
            try { minutesHide = int.Parse(sMinutesHide); }
            catch (Exception) { minutesHide = 5; }

            TimeSpan visibilityTimeout = new TimeSpan(0, minutesHide, 0);

            QueueClient queue = prepareQueue(key_dir);
            Response<Azure.Storage.Queues.Models.QueueMessage[]> messages = queue.ReceiveMessages(maxMessages, visibilityTimeout);
            for( int i = 0; i < messages.Value.Length; i++)
            {
                Azure.Storage.Queues.Models.QueueMessage message = (Azure.Storage.Queues.Models.QueueMessage)messages.Value.GetValue(i);
                string message_json = string.Format(template_message, message.MessageId, message.InsertedOn, message.ExpiresOn, message.PopReceipt, message.NextVisibleOn,
                    message.DequeueCount, message.Body);

                if (messages_json.Length > 0)
                    messages_json += ",";

                messages_json += message_json;
            }
            return string.Format(template_queue, messages_json);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key_dir"></param>
        /// <returns></returns>
        private static QueueClient prepareQueue(string key_dir)
        {
            string storage = Utils.GetEnvironmentVariable("Upload_Storage");          // bruweadls001
            string storageKey = Utils.GetEnvironmentVariable("Upload_StorageKey");
            StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(storage, storageKey);     // Сформировать удостоверение

            QueueClient queue = new QueueClient(Utils.GetUriQueue(storage, key_dir), sharedKeyCredential);
            return queue;
        }
    }
}
