using System;
using System.Collections.Generic;
using System.Text;

namespace Awara.Retail.Adept.UploadFiles
{
    public static class Utils
    {
        /// <summary>
        /// Генерация URI к ADLS
        /// </summary>
        /// <param name="accountName"></param>
        /// <returns></returns>
        public static Uri GetUriADLS(string accountName)
        {
            return new Uri(string.Format("https://{0}.dfs.core.windows.net/", accountName));
        }
        /// <summary>
        /// Генерация URI к службе очередей
        /// </summary>
        /// <param name="accountName"></param>
        /// <returns></returns>
        public static Uri GetUriQueue(string accountName, string queueName)
        {   //https://{account_name}.queue.core.windows.net/{queue_name}
            return new Uri(string.Format("https://{0}.queue.core.windows.net/{1}", accountName, queueName));
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static string GetEnvironmentVariable(string name)
        {
            return System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
        }
    }
}
