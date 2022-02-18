using System;
using System.Collections.Generic;
using System.Text;
using static Awara.Retail.Adept.UploadFiles.Utils;
using System.Data.SqlClient;
using System.Data;

namespace Awara.Retail.Adept.UploadFiles
{
    public class Dalc
    {
        SqlConnectionStringBuilder builder = null;

        #region TransportClasses
        /// <summary>
        /// Транспортный класс переоценки примера
        /// </summary>
        public class OverEstimate
        {
            public string pid { get; set; }
            public string filename { get; set; }
            public string chapter { get; set; }
            public string paragraph { get; set; }
            public string textHash { get; set; }
            public int scoreB { get; set; }
            public int? scoreM { get; set; }
            public string user { get; set; }
        }
        public class AgreementFile
        {
            public AgreementFile(string _file)
            {
                file = _file;
            }
            public string file { get; set; }
        }
        /// <summary>
        /// Транспортный класс запроса результатов прогноза
        /// </summary>
        public class RequestResults
        {
            public string pid { get; set; }
            //public AgreementFile[] filenames { get; set; }
            //public string GetFiles()
            //{
                //string files = "";
                //foreach(AgreementFile agreementFile in filenames)
                //{
                    //if (files.Length != 0) files += ",";
                    //files += string.Format("N'{0}'", agreementFile.file);
                //}
                //return files;
            //}
        }
        public class RequestResultsSmpl
        {
            public string pid { get; set; }
            public string[] filenames { get; set; }
            public string GetFiles()
            {
                string files = "";
                foreach (string agreementFile in filenames)
                {
                    if (files.Length != 0) files += ",";
                    files += string.Format("N'{0}'", agreementFile);
                }
                return files;
            }
        }
        #endregion


        public Dalc()
        {
            builder = new SqlConnectionStringBuilder
            {
                DataSource = Utils.GetEnvironmentVariable("Db_sqlserver"),
                UserID = Utils.GetEnvironmentVariable("Db_user"),
                Password = Utils.GetEnvironmentVariable("Db_password"),
                InitialCatalog = Utils.GetEnvironmentVariable("Db_sqldb")
            };
        }
        /// <summary>
        /// Чтение даты сгруппированной до минуты для создания пакетов json c несколькими сообщениями
        /// </summary>
        /// <returns>dataParts</returns>
        public void SetOverEstimate(OverEstimate estimate)
        {
            using SqlConnection connection = new SqlConnection(builder.ConnectionString);
            using (SqlCommand cmd = new SqlCommand("dbo.usp_overestimate", connection))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@pid", SqlDbType.VarChar).Value = estimate.pid;
                cmd.Parameters.Add("@filename", SqlDbType.NVarChar).Value = estimate.filename;
                cmd.Parameters.Add("@chapter", SqlDbType.NVarChar).Value = estimate.chapter;
                cmd.Parameters.Add("@paragraph", SqlDbType.NVarChar).Value = estimate.paragraph;
                cmd.Parameters.Add("@textHash", SqlDbType.NVarChar).Value = estimate.textHash;
                cmd.Parameters.Add("@scoreB", SqlDbType.Int).Value = estimate.scoreB;
                if (estimate.scoreM != null)
                    cmd.Parameters.Add("@scoreM", SqlDbType.Int).Value = estimate.scoreM;
                cmd.Parameters.Add("@user", SqlDbType.NVarChar).Value = estimate.user;

                connection.Open();
                cmd.ExecuteNonQuery();
            }
        }
        /// <summary>
        /// Чтение данных с прогнозами в формате json из базы данных
        /// </summary>
        /// <returns>dataParts</returns>
        public string GetResultsByPID(string pid)
        {
            string results="";
            using SqlConnection connection = new SqlConnection(builder.ConnectionString);
            using (SqlCommand cmd = new SqlCommand("dbo.[usp_getResultsByPID]", connection))
            {
                cmd.CommandType = CommandType.StoredProcedure;

                cmd.Parameters.AddWithValue("@pid", pid);

                cmd.Parameters.Add("@json", SqlDbType.NVarChar, 100000000);
                cmd.Parameters["@json"].Direction = ParameterDirection.Output;

                connection.Open();
                cmd.ExecuteNonQuery();

                if (!DBNull.Value.Equals(cmd.Parameters["@json"].Value))
                {
                    results = (string)cmd.Parameters["@json"].Value;
                }
                else
                    results = "Данные не найдены";
            }
            return results;
        }
        /// <summary>
        /// Чтение данных с прогнозами в формате json из базы данных
        /// </summary>
        /// <returns>dataParts</returns>
        public string GetFiles(string dateBegin, string dateEnd)
        {
            string results = "";
            using SqlConnection connection = new SqlConnection(builder.ConnectionString);
            using (SqlCommand cmd = new SqlCommand("dbo.[usp_getFiles]", connection))
            {
                cmd.CommandType = CommandType.StoredProcedure;

                if (dateBegin != null)
                    cmd.Parameters.AddWithValue("@dateBegin", dateBegin);

                if (dateEnd != null)
                    cmd.Parameters.AddWithValue("@dateEnd", dateEnd);

                cmd.Parameters.Add("@json", SqlDbType.NVarChar, 100000000);
                cmd.Parameters["@json"].Direction = ParameterDirection.Output;

                connection.Open();
                cmd.ExecuteNonQuery();

                if (!DBNull.Value.Equals(cmd.Parameters["@json"].Value))
                {
                    results = (string)cmd.Parameters["@json"].Value;
                }
                else
                    results = "Данные не найдены";
            }
            return results;
        }
    }
}
