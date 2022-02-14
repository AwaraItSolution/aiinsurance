using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;
using Newtonsoft.Json;

namespace Awara.Retail.Adept.UploadFiles
{
    /// <summary>
    /// Класс описывающий сообщение в очереди процессинга файлов соглашений
    /// </summary>
    [JsonObject(Title= "QueueMessage")]
    class QueueMessage
    {
        public int state=0;
        public string message="";

        public QueueMessage(int _state, string _message)
        {
            this.state = _state;
            this.message = _message;
        }
        //[JsonPropertyName("message")]
        //public string Message { get => message; set => message = value; }
        //[JsonPropertyName("state")]
        //public int State { get => state; set => state = value; }

    }
}
