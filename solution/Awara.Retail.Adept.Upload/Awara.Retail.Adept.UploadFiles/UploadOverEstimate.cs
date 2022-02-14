using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using static Awara.Retail.Adept.UploadFiles.Dalc;

namespace Awara.Retail.Adept.UploadFiles
{
    public static class UploadOverEstimate
    {
        [FunctionName("UploadOverEstimate")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody;
            IActionResult result;

            try
            {
                string method = req.Method;

                log.LogInformation(string.Format("method: {0}", method));

                if (method == "POST")
                {
                    requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                    OverEstimate overEstimate = JsonConvert.DeserializeObject<OverEstimate>(requestBody);

                    Dalc dalc = new Dalc();
                    dalc.SetOverEstimate(overEstimate);

                    result = new OkObjectResult("Ok");
                }
                else if (method == "GET")
                {

                    string key_dir = req.Query["key-dir"];

                    if (string.IsNullOrEmpty(key_dir))
                        throw new Exception("Parameter key-dir is empty.");

                    Dalc dalc = new Dalc();
                    string results = dalc.GetResultsByPID(key_dir);

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
