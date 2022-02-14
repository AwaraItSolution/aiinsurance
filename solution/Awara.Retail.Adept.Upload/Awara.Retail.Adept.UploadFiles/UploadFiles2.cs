using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

using Azure.Storage;
using Azure.Storage.Files.DataLake;
using System.Collections.Generic;
using Azure.Storage.Queues;
using System.Diagnostics;
using System.Net.Http;
using System.Net;
using Azure.Storage.Files.DataLake.Models;
using Azure;
using Azure.Storage.Files.Shares;

/// <summary>
/// Задачи функции:
/// 1. Перехватить POST запрос на передачу файлов в облако,
/// 2. Организовать очередь в ADLS для передачи через нее информации о процессе обработки файлов клиенту,
/// 3. Скопировать файлы в ADLS,
/// 4. Послать POST запрос, который поймает logic app и запустит конвейер ADF по обработке принятых файлов,
/// 5. Вернуть, в случае успеха, ключ, по которому будет осуществляться запрос клиентского приложения к очереди для получения текущего статуса обработки.
/// </summary>
namespace Awara.Retail.Adept.UploadFiles
{
    public static class UploadFiles2
    {
        #region Members
        private static string container = "";
        private static string basePath = "";
        private static string storage = "";
        private static string subPathConverted = "";
        private static string subPathLanded = "";
        private static string subPathProcessed = "";
        private static ProcessId procId;
        private static string urlPostADF = "";
        private static string urlQueue = "";
        private static string filePath = "";
        /// <summary>
        /// Стадии выполнения процесса
        /// </summary>
        private enum StateProcess
        {
            Initialize = 0,
            СopyingFiles = 10,
            FilesCopied = 20,
            RunDataFactory = 30,
            DataFactoryRunned = 40,
            FilesConverted = 50,
            FilesForecasting = 55,
            FilesProcessed = 60,
            Cancel = 190,
            Finished = 200
        };
        private static StateProcess state;

        private static IFormFileCollection files;
        private static StorageSharedKeyCredential sharedKeyCredential;
        private static DataLakeServiceClient serviceClient;
        private static DataLakeFileSystemClient filesystem;
        private static DataLakeDirectoryClient directoryClient;
        //private static QueueClient queue;
        #endregion
        #region SubClasses
        /// <summary>
        /// Транспортный класс результата выполнения асинхронной операции
        /// </summary>
        private class AsyncResult
        {
            public bool succes;
            public string message="";

            public AsyncResult()
            {
                succes = false;
            }
            public AsyncResult(bool _succes, string _message)
            {
                succes = _succes;
                message = _message;
            }

        }
        /// <summary>
        /// 
        /// </summary>
        private class ProcessId
        {
            public string id = "";
            private ProcessId()
            {
                id = "";
            }
            public ProcessId(string _id)
            {
                id = _id;
            }
            public string getProcessIdByJson()
            {
                return "{"+string.Format("\"pid\": \"{0}\"", id)+"}";
            }
        }
        #endregion
        [FunctionName("UploadFiles2")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            IActionResult result;

            try
            {
                AsyncResult response;
                string method = req.Method;

                log.LogInformation(string.Format("method: {0}", method));

                if (method == "POST")
                {
                    files = req.Form.Files;
                    if (files.Count == 0)
                        throw new Exception("Файлы для загрузки отсутствуют");

                    response = await Initialize(state, log, method);
                    if (!response.succes)
                        throw new Exception(response.message);

                    state = StateProcess.СopyingFiles;
                    if (!await UploadToADLS(files, log, state))
                        throw new Exception("Файлы не загружены");

                    state = StateProcess.FilesCopied;

                    response = await SendPost4Adf();

                    if (response.succes)
                    {
                        state = StateProcess.RunDataFactory;
                        await FixHistory(state, log, string.Format("POST запрос на запуск фабрики отправлен. {0}", response.message));
                    }
                    else
                    {
                        await FixHistory(state, log, response.message);
                        state = StateProcess.Cancel;
                        UploadCancel(state, log);
                        throw new Exception(response.message);
                    }

                    result = new OkObjectResult(procId.getProcessIdByJson());
                }
                else if (method == "GET")
                {
                    string key_dir = req.Query["key-dir"];

                    if (string.IsNullOrEmpty(key_dir))
                        throw new Exception("Parameter key-dir is empty.");

                    response = await Initialize(state, log, method, key_dir);

                    result = await getResult(key_dir, log);
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
        #region Initial
        /// <summary>
        /// Получить доступ к ADLS, создать папку для выгрузки файлов
        /// Создать очередь для мониторинга обработки
        /// </summary>
        private static async Task<AsyncResult> Initialize(StateProcess state, ILogger log, string method, string key_dir="")
        {
            string storageKey;
            AsyncResult result;

            state = StateProcess.Initialize;
            
            urlPostADF = Utils.GetEnvironmentVariable("Upload_UrlPostADF");    // URL для POST запроса для активации конвейера ADF
            urlQueue = Utils.GetEnvironmentVariable("Upload_UrlQueue");        // URL для операций с очередью
            container = Utils.GetEnvironmentVariable("Upload_Container");      // adept
            basePath = Utils.GetEnvironmentVariable("Upload_BasePath");        // UDL/Internal Sources/Manual Files/Agreements/Landed/
            storage = Utils.GetEnvironmentVariable("Upload_Storage");          // bruweadls001
            storageKey = Utils.GetEnvironmentVariable("Upload_StorageKey");
            subPathConverted = Utils.GetEnvironmentVariable("Upload_SubPathConverted");   // Converted/
            subPathLanded = Utils.GetEnvironmentVariable("Upload_SubPathLanded");         // Landed/
            subPathProcessed = Utils.GetEnvironmentVariable("Upload_SubPathProcessed");   // Processed/
            
            log.LogInformation(string.Format("container: {0}", container));
            log.LogInformation(string.Format("basePath: {0}", basePath));
            log.LogInformation(string.Format("storage: {0}", storage));
            log.LogInformation(string.Format("subPathConverted: {0}", subPathConverted));
            log.LogInformation(string.Format("subPathLanded: {0}", subPathLanded));
            log.LogInformation(string.Format("subPathProcessed: {0}", subPathProcessed));
            log.LogInformation(string.Format("urlQueue: {0}", urlQueue));

            sharedKeyCredential = new StorageSharedKeyCredential(storage, storageKey);                  // Сформировать удостоверение
            serviceClient = new DataLakeServiceClient(Utils.GetUriADLS(storage), sharedKeyCredential);  // Получить доступ к Data Storage
            filesystem = serviceClient.GetFileSystemClient(container);                                  // Получить контейнер

            if (method == "POST")
            {
                procId = new ProcessId(string.Format("{0}{1}", DateTime.Now.ToString(("yyyy-MM-dd-HH-mm-ss-")), Guid.NewGuid()));
                filePath = Path.Combine(basePath, subPathLanded, procId.id);
                directoryClient = filesystem.CreateDirectory(filePath);                                 // Создать папку назначения
                result = await CreateQueue();
                await FixHistory(state, log, "Очередь создана");
            }
            else
            {
                procId = new ProcessId(key_dir);
                filePath = Path.Combine(basePath, subPathProcessed, procId.id);
                directoryClient = filesystem.GetDirectoryClient(filePath);      // Получить папку назначения
                result = new AsyncResult(true, "Status: OK");
            }
            log.LogInformation(string.Format("dirKey: {0}", procId.id));
            log.LogInformation(string.Format("filePath: {0}", filePath));
            try
            {
                bool isDirectory = directoryClient.GetProperties().Value.IsDirectory;
            }
            catch (Exception)
            {
                throw new Exception(string.Format("Каталог {0} отсутствует", filePath));
            }

            return result;
        }
        #endregion
        #region Methods Async
        private static async Task<AsyncResult> CreateQueue()
        {
            AsyncResult result;
            Dictionary<string, string> parameters = new Dictionary<string, string>
            {
                { "command", "create" },
                { "key-dir", procId.id }
            };
            Uri fullUrl = new Uri(Microsoft.AspNetCore.WebUtilities.QueryHelpers.AddQueryString(urlQueue, parameters));

            HttpClientHandler handler = new HttpClientHandler();
            HttpClient httpClient = new HttpClient(handler);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, fullUrl);
            HttpResponseMessage response = await httpClient.SendAsync(request);
            if (response.StatusCode == HttpStatusCode.OK || response.StatusCode == HttpStatusCode.Accepted)

                result = new AsyncResult(true, string.Format("Status: {0}", response.StatusCode));

            else
                result = new AsyncResult(false, string.Format("POST запрос создания очереди не выполнен. StatusCode: {0}. Content: {1}", response.StatusCode, response.Content.ToString()));

            return result;
        }
        private static async Task<AsyncResult> PutQueue(string message)
        {
            AsyncResult result;
            Dictionary<string, string> parameters = new Dictionary<string, string>
            {
                { "command", "put" },
                { "key-dir", procId.id },
                { "message", message}
            };
            Uri fullUrl = new Uri(Microsoft.AspNetCore.WebUtilities.QueryHelpers.AddQueryString(urlQueue, parameters));

            HttpClientHandler handler = new HttpClientHandler();
            HttpClient httpClient = new HttpClient(handler);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, fullUrl);
            HttpResponseMessage response = await httpClient.SendAsync(request);
            if (response.StatusCode == HttpStatusCode.OK || response.StatusCode == HttpStatusCode.Accepted)

                result = new AsyncResult(true, string.Format("Status: {0}", response.StatusCode));

            else
                result = new AsyncResult(false, string.Format("POST запрос добавления в очередь не выполнен. StatusCode: {0}. Content: {1}", response.StatusCode, response.Content.ToString()));

            return result;
        }
        /// <summary>
        /// Послать POST запрос, который перехватит LogicApp и запустит конвейер ADF
        /// </summary>
        private static async Task<AsyncResult> SendPost4Adf()
        {
            AsyncResult result;
            Dictionary<string, string> parameters = new Dictionary<string, string>
            {
                { "container", container },
                { "storage", storage },
                { "basePath", basePath },
                { "subPathLanded", subPathLanded },
                { "subPathConverted", subPathConverted },
                { "subPathProcessed", subPathProcessed },
                { "keyDirectory", procId.id },
                { "urlQueue", urlQueue }
            };
            Uri fullUrl = new Uri(Microsoft.AspNetCore.WebUtilities.QueryHelpers.AddQueryString(urlPostADF, parameters));

            HttpClientHandler handler = new HttpClientHandler();
            HttpClient httpClient = new HttpClient(handler);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, fullUrl);
            HttpResponseMessage response = await httpClient.SendAsync(request);
            if (response.StatusCode == HttpStatusCode.OK || response.StatusCode == HttpStatusCode.Accepted)

                result =  new AsyncResult(true, string.Format("Status: {0}", response.StatusCode));

            else
                result = new AsyncResult(false, string.Format("POST запрос к ADF не выполнен. StatusCode: {0}. Проверьте на портале включен ли LogicApp. Content: {1}", response.StatusCode, response.Content.ToString()));

            return result;
        }
        /// <summary>
        /// Послать POST запрос, который перехватит LogicApp и запустит конвейер ADF
        /// </summary>
        private static async Task<AsyncResult> SendPost4Queue(string message)
        {
            AsyncResult result;
            Dictionary<string, string> parameters = new Dictionary<string, string>
            {
                { "container", container },
                { "storage", storage },
                { "basePath", basePath },
                { "subPathLanded", subPathLanded },
                { "subPathConverted", subPathConverted },
                { "subPathProcessed", subPathProcessed },
                { "keyDirectory", procId.id }
            };
            Uri fullUrl = new Uri(urlPostADF + message);

            HttpClientHandler handler = new HttpClientHandler();
            HttpClient httpClient = new HttpClient(handler);
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, fullUrl);
            HttpResponseMessage response = await httpClient.SendAsync(request);
            if (response.StatusCode == HttpStatusCode.OK || response.StatusCode == HttpStatusCode.Accepted)

                result = new AsyncResult(true, string.Format("Status: {0}", response.StatusCode));

            else
                result = new AsyncResult(false, string.Format("POST запрос к ADF не выполнен. StatusCode: {0}. Проверьте на портале включен ли LogicApp. Content: {1}", response.StatusCode, response.Content.ToString()));

            return result;
        }
        /// <summary>
        /// Вернуть поток с файлом результата
        /// </summary>
        /// <param name="keyDirectory">название каталога и файла в котором хранится результат</param>
        /// <param name="log"></param>
        /// <returns></returns>
        private static async Task<IActionResult> getResult(string keyDirectory, ILogger log)
        {
            string file_result = string.Format("{0}.zip", keyDirectory);

            DataLakeFileClient fileClient = directoryClient.GetFileClient(file_result);

            try
            {
                bool isDirectory = fileClient.GetProperties().Value.IsDirectory;
            }
            catch (Exception)
            {
                throw new Exception(string.Format("Файл результата {0} отсутствует в папке {1}", file_result, filePath));
            }

            Stream stream = await fileClient.OpenReadAsync();
            
            return new FileStreamResult(stream, "application/octet-stream") { FileDownloadName = file_result };
        }
        /// <summary>
        /// Вернуть строку json c результатом прогноза
        /// </summary>
        /// <param name="keyDirectory">название каталога и файла в котором хранится результат</param>
        /// <param name="log"></param>
        /// <returns></returns>
        private static async Task<IActionResult> getResultJson(string keyDirectory, ILogger log)
        {
            string file_result = string.Format("{0}.zip", keyDirectory);

            DataLakeFileClient fileClient = directoryClient.GetFileClient(file_result);

            try
            {
                bool isDirectory = fileClient.GetProperties().Value.IsDirectory;
            }
            catch (Exception)
            {
                throw new Exception(string.Format("Файл результата {0} отсутствует в папке {1}", file_result, filePath));
            }

            Stream stream = await fileClient.OpenReadAsync();

            return new FileStreamResult(stream, "application/octet-stream") { FileDownloadName = file_result };
        }
        /// <summary>
        /// Загрузка файлов в озеро
        /// </summary>
        /// <param name="files">коллекция загружаемых файлов</param>
        /// <param name="log">объект логирования</param>
        /// <returns></returns>
        private static async Task<bool> UploadToADLS(IFormFileCollection files, ILogger log, StateProcess state)
        {
            int i = 0;
            long size = 0;
            bool result = false;
            foreach (IFormFile formFile in files)
            {
                if (formFile.Length > 0)
                {
                    string extFile = Path.GetExtension(formFile.FileName);
                    if (extFile == ".pdf" || extFile == ".json")
                    {
                        DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(formFile.FileName);
                        // Append data to the DataLake File
                        await fileClient.AppendAsync(formFile.OpenReadStream(), offset: 0);
                        await fileClient.FlushAsync(formFile.Length);

                        await FixHistory(state, log, string.Format("Файл: {0} размером: {1} - загружен.", formFile.FileName, formFile.Length));

                        i++; size += formFile.Length;
                    }
                    else
                        await FixHistory(state, log, string.Format("Файл: {0} - пропущен. Требуется PDF файл.", formFile.FileName, formFile.Length));
                }
            }
            if (size > 0)
            {
                await FixHistory(state, log, string.Format("Загрузка завершена. Загружено:{0} файлов, размером:{1} байт.", i, size));
                result = true;
            }
            return result;
        }
        #endregion
        #region Methods Sync
        /// <summary>
        /// Удаление загруженных файлов через удаление каталога загрузки
        /// </summary>
        /// <param name="state"></param>
        /// <param name="log"></param>
        private static void UploadCancel(StateProcess state, ILogger log)
        {
            try
            {
                string message = "UploadCancel: Удаление загруженных файлов.";
                FixHistory(state, log, message);

                DataLakeDirectoryClient directoryClient = filesystem.GetDirectoryClient(filePath);
                directoryClient.Delete();
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
            }
        }
        #endregion
        #region Methods Utilitarian
        /// <summary>
        /// Запись сообщений в лог приложения и в очередь сообщений
        /// </summary>
        /// <param name="state"></param>
        /// <param name="log"></param>
        /// <param name="message"></param>
        private static async Task<bool> FixHistory(StateProcess state, ILogger log, string message="")
        {
            if (message != null)
            {
                QueueMessage qm = new QueueMessage(((int)state), message);
                string fmessage = JsonConvert.SerializeObject(qm, Formatting.Indented);
                //string fmessage = string.Format("State:{0} {1}", state.ToString("D"), message);
                log.LogInformation(fmessage);
                //if (queue != null)
                //  queue.SendMessage(fmessage);
                await PutQueue(fmessage);
            }
            return true;
        }
        #endregion
    }
}
