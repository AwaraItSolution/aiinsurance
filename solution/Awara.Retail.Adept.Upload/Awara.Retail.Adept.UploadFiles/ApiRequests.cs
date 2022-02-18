using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Awara.Retail.Adept.UploadFiles
{
    public static class ApiRequests
    {
        [FunctionName("ApiRequests")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            //string name = req.Query["name"];
            IActionResult result;
            try
            {
                string method = req.Method;

                log.LogInformation(string.Format("method: {0}", method));

                if (method == "GET")
                {
                    string content = req.Query["content"];

                    if (string.IsNullOrEmpty(content))
                        throw new Exception("Parameter 'content' is empty.");

                    Dalc dalc = new Dalc();
                    string results;
                    switch (content)
                    {
                        case "files":
                            string dateBegin = req.Query["date-begin"];
                            string dateEnd = req.Query["date-end"];
                            results = dalc.GetFiles(dateBegin, dateEnd);
                            break;
                        default:
                            throw new Exception(string.Format("Parameter value 'content'={0} not defined.", method));
                    }

                    

                    result = new OkObjectResult(results);
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
    }
}
