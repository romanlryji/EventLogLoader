using MySql.Data.MySqlClient;
//using Nest;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Threading;

namespace EventLogApp
{
    partial class EventLogProcessor
    {
        public EventElementsDictionary<User> UsersDictionary = new EventElementsDictionary<User>();
        public EventElementsDictionary<Computer> ComputersDictionary = new EventElementsDictionary<Computer>();
        public EventElementsDictionary<Application> ApplicationsDictionary = new EventElementsDictionary<Application>();
        public EventElementsDictionary<EventType> EventsDictionary = new EventElementsDictionary<EventType>();
        public EventElementsDictionary<Metadata> MetadataDictionary = new EventElementsDictionary<Metadata>();
        public EventElementsDictionary<Server> ServersDictionary = new EventElementsDictionary<Server>();
        public EventElementsDictionary<MainPort> MainPortsDictionary = new EventElementsDictionary<MainPort>();
        public EventElementsDictionary<SecondPort> SecondPortsDictionary = new EventElementsDictionary<SecondPort>();

        public List<OneEventRecord> EventsList = new List<OneEventRecord>();

        public string ESIndexName;
        public string ESServerName;

        public string InfobaseName;
        public string InfobaseGuid;
        public int InfobaseID;
        public string ConnectionString;
        public bool ItIsMSSQL;
        public bool ItIsMySQL;
        public bool ItIsElasticSearch;
        public bool ESUseSynonymsForFieldsNames;
        public DateTime LoadEventsStartingAt;
        public ElasticSearchFieldSynonymsClass ESFieldSynonyms = new ElasticSearchFieldSynonymsClass();
        public int SleepTime = 60 * 1000;// '1 минута

        public NLog.Logger Log;

        public string Catalog;
        long CurrentPosition;
        string CurrentFilename;
        long LastEventNumber83;
        public DateTime LastReferenceUpdate;


        public void AddUser(long code, string guid, string name)
        {
            this.UsersDictionary.Add(new User(code, name, guid));
        }


        public void AddComputer(long code, string name)
        {
            this.ComputersDictionary.Add(new Computer(code, name));
        }


        public void AddApplication(long code, string name)
        {
            this.ApplicationsDictionary.Add(new Application(code, name));
        }


        public void AddEvent(long code, string name)
        {
            this.EventsDictionary.Add(new EventType(code, name));
        }


        public void AddMetadata(long code, string guid, string name)
        {
            this.MetadataDictionary.Add(new Metadata(code, name, guid));
        }


        public void AddServer(long code, string name)
        {
            this.ServersDictionary.Add(new Server(code, name));
        }


        public void AddMainPort(long code, string name)
        {
            this.MainPortsDictionary.Add(new MainPort(code, name));
        }


        public void AddSecondPort(long code, string name)
        {
            this.SecondPortsDictionary.Add(new SecondPort(code, name));
        }


        public void SaveReferenceValuesToDatabase()
        {
            if (ItIsMSSQL)
            {
                SqlConnection objConn = new SqlConnection(ConnectionString);
                objConn.Open();

                SqlCommand command = new SqlCommand("IF NOT EXISTS (select * from [dbo].[Applications] where [Code] = @v1 AND [InfobaseCode] = @v3) " +
                                          "INSERT INTO [dbo].[Applications] ([InfobaseCode],[Code],[Name]) VALUES(@v3, @v1, @v2)", objConn);

                foreach (var item in ApplicationsDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Int)).Value = item.Value.Code;
                        command.Parameters.Add(new SqlParameter("@v2", SqlDbType.Char)).Value = item.Value.Name;
                        command.Parameters.Add(new SqlParameter("@v3", SqlDbType.Int)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "IF NOT EXISTS (select * from [dbo].[Computers] where [Code] = @v1 AND [InfobaseCode] = @v3) " +
                                    "INSERT INTO [dbo].[Computers] ([InfobaseCode],[Code],[Name]) VALUES(@v3, @v1,@v2)";

                foreach (var item in ComputersDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Int)).Value = item.Value.Code;
                        command.Parameters.Add(new SqlParameter("@v2", SqlDbType.Char)).Value = item.Value.Name;
                        command.Parameters.Add(new SqlParameter("@v3", SqlDbType.Int)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "IF NOT EXISTS (select * from [dbo].[EventsType] where [Code] = @v1 AND [InfobaseCode] = @v3) " +
                                    "INSERT INTO [dbo].[EventsType] ([InfobaseCode],[Code],[Name]) VALUES(@v3, @v1,@v2)";

                foreach (var item in EventsDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Int)).Value = item.Value.Code;
                        command.Parameters.Add(new SqlParameter("@v2", SqlDbType.Char)).Value = item.Value.Name;
                        command.Parameters.Add(new SqlParameter("@v3", SqlDbType.Int)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "IF NOT EXISTS (select * from [dbo].[MainPorts] where [Code] = @v1 AND [InfobaseCode] = @v3) " +
                                    "INSERT INTO [dbo].[MainPorts] ([InfobaseCode],[Code],[Name]) VALUES(@v3, @v1,@v2)";

                foreach (var item in MainPortsDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Int)).Value = item.Value.Code;
                        command.Parameters.Add(new SqlParameter("@v2", SqlDbType.Char)).Value = item.Value.Name;
                        command.Parameters.Add(new SqlParameter("@v3", SqlDbType.Int)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = @"MERGE INTO [dbo].[Metadata] AS Target
                                    USING(SELECT @v1 AS[Code],
                                                    @v4 AS[InfobaseCode],
                                                    @v3 AS[Guid]) AS Source
                                    ON(Target.[Code] = Source.[Code]

                                        AND Target.[InfobaseCode] = Source.[InfobaseCode]

                                        AND Target.[Guid] = Source.[Guid])
                                    WHEN MATCHED AND NOT([Name] = @v2) THEN UPDATE
                                    SET[Name] = @v2
                                    WHEN NOT MATCHED THEN INSERT([InfobaseCode], [Code], [Name], [Guid]) VALUES(@v4, @v1, @v2, @v3);";

                foreach (var item in MetadataDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Int)).Value = item.Value.Code;
                        command.Parameters.Add(new SqlParameter("@v2", SqlDbType.Char)).Value = item.Value.Name;
                        command.Parameters.Add(new SqlParameter("@v3", SqlDbType.Char)).Value = item.Value.Guid;
                        command.Parameters.Add(new SqlParameter("@v4", SqlDbType.Int)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = @"MERGE INTO [dbo].[Users] AS Target
                                    USING(SELECT @v1 AS[Code],
                                                  @v4 AS[InfobaseCode],
                                                  @v3 AS[Guid]) AS Source
                                    ON(Target.[Code] = Source.[Code]

                                        AND Target.[InfobaseCode] = Source.[InfobaseCode]

                                        AND Target.[Guid] = Source.[Guid])
                                    WHEN MATCHED AND NOT([Name] = @v2) THEN UPDATE
                                    SET[Name] = @v2
                                    WHEN NOT MATCHED THEN INSERT([InfobaseCode], [Code], [Name], [Guid]) VALUES(@v4, @v1, @v2, @v3);";

                foreach (var item in UsersDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Int)).Value = item.Value.Code;
                        command.Parameters.Add(new SqlParameter("@v2", SqlDbType.Char)).Value = item.Value.Name;
                        command.Parameters.Add(new SqlParameter("@v3", SqlDbType.Char)).Value = item.Value.Guid;
                        command.Parameters.Add(new SqlParameter("@v4", SqlDbType.Int)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "IF NOT EXISTS (select * from [dbo].[SecondPorts] where [Code] = @v1 AND [InfobaseCode] = @v3) " +
                                    "INSERT INTO [dbo].[SecondPorts] ([InfobaseCode],[Code],[Name]) VALUES(@v3, @v1,@v2)";

                foreach (var item in SecondPortsDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Int)).Value = item.Value.Code;
                        command.Parameters.Add(new SqlParameter("@v2", SqlDbType.Char)).Value = item.Value.Name;
                        command.Parameters.Add(new SqlParameter("@v3", SqlDbType.Int)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "IF NOT EXISTS (select * from [dbo].[Servers] where [Code] = @v1 AND [InfobaseCode] = @v3) " +
                                    "INSERT INTO [dbo].[Servers] ([InfobaseCode],[Code],[Name]) VALUES(@v3, @v1,@v2)";

                foreach (var item in ServersDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Int)).Value = item.Value.Code;
                        command.Parameters.Add(new SqlParameter("@v2", SqlDbType.Char)).Value = item.Value.Name;
                        command.Parameters.Add(new SqlParameter("@v3", SqlDbType.Int)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "IF NOT EXISTS (select * from [dbo].[Infobases] where [Guid] = @v0) " +
                                    "INSERT INTO [dbo].[Infobases] ([Guid],[Code],[Name]) VALUES(@v0,@v1,@v2)";

                try
                {
                    command.Parameters.Clear();
                    command.Parameters.Add(new SqlParameter("@v0", SqlDbType.Char)).Value = InfobaseGuid;
                    command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Int)).Value = InfobaseID;
                    command.Parameters.Add(new SqlParameter("@v2", SqlDbType.Char)).Value = InfobaseName;
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

                command.Dispose();
                objConn.Close();
                objConn.Dispose();
            }
            else if (ItIsMySQL)
            {
                MySqlConnection objConn = new MySqlConnection(ConnectionString);
                objConn.Open();

                MySqlCommand command = new MySqlCommand("REPLACE INTO `Applications`(`InfobaseCode`, `Code`, `Name`) VALUES(@v3, @v1, @v2)", objConn);

                foreach (var item in ApplicationsDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.Int32)).Value = item.Value.Code;
                        command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = item.Value.Name;
                        command.Parameters.Add(new MySqlParameter("@v3", MySqlDbType.Int32)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "REPLACE INTO `Computers` (`InfobaseCode`,`Code`,`Name`) VALUES(@v3, @v1, @v2)";

                foreach (var item in ComputersDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.Int32)).Value = item.Value.Code;
                        command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = item.Value.Name;
                        command.Parameters.Add(new MySqlParameter("@v3", MySqlDbType.Int32)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "REPLACE INTO `EventsType` (`InfobaseCode`,`Code`,`Name`) VALUES(@v3, @v1,@v2)";

                foreach (var item in EventsDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.Int32)).Value = item.Value.Code;
                        command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = item.Value.Name;
                        command.Parameters.Add(new MySqlParameter("@v3", MySqlDbType.Int32)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "REPLACE INTO `MainPorts` (`InfobaseCode`,`Code`,`Name`) VALUES(@v3, @v1,@v2)";

                foreach (var item in MainPortsDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.Int32)).Value = item.Value.Code;
                        command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = item.Value.Name;
                        command.Parameters.Add(new MySqlParameter("@v3", MySqlDbType.Int32)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = @"REPLACE INTO `Metadata` (`InfobaseCode`,`Code`,`Name`,`Guid`) VALUES(@v4, @v1,@v2,@v3)";

                foreach (var item in MetadataDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.Int32)).Value = item.Value.Code;
                        command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = item.Value.Name;
                        command.Parameters.Add(new MySqlParameter("@v3", MySqlDbType.VarChar)).Value = item.Value.Guid;
                        command.Parameters.Add(new MySqlParameter("@v4", MySqlDbType.Int32)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = @"REPLACE INTO `Users` (`InfobaseCode`,`Code`,`Name`,`Guid`) VALUES(@v4, @v1,@v2,@v3)";

                foreach (var item in UsersDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.Int32)).Value = item.Value.Code;
                        command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = item.Value.Name;
                        command.Parameters.Add(new MySqlParameter("@v3", MySqlDbType.VarChar)).Value = item.Value.Guid;
                        command.Parameters.Add(new MySqlParameter("@v4", MySqlDbType.Int32)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "REPLACE INTO `SecondPorts` (`InfobaseCode`,`Code`,`Name`) VALUES(@v3, @v1,@v2)";

                foreach (var item in SecondPortsDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.Int32)).Value = item.Value.Code;
                        command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = item.Value.Name;
                        command.Parameters.Add(new MySqlParameter("@v3", MySqlDbType.Int32)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "REPLACE INTO `Servers` (`InfobaseCode`,`Code`,`Name`) VALUES(@v3, @v1,@v2)";

                foreach (var item in ServersDictionary)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.Int32)).Value = item.Value.Code;
                        command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = item.Value.Name;
                        command.Parameters.Add(new MySqlParameter("@v3", MySqlDbType.Int32)).Value = InfobaseID;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                command.CommandText = "REPLACE INTO `Infobases` (`Guid`,`Code`,`Name`) VALUES(@v0,@v1,@v2)";

                try
                {
                    command.Parameters.Clear();
                    command.Parameters.Add(new MySqlParameter("@v0", MySqlDbType.VarChar)).Value = InfobaseGuid;
                    command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.Int32)).Value = InfobaseID;
                    command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = InfobaseName;
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

                command.Dispose();
                objConn.Close();
                objConn.Dispose();
            }

            LastReferenceUpdate = DateTime.Now;
        }


        public void GetInfobaseIDFromDatabase()
        {
            if (ItIsMSSQL)
            {
                SqlConnection objConn = new SqlConnection(ConnectionString);
                objConn.Open();

                SqlCommand command = new SqlCommand("SELECT [Code] FROM [dbo].[Infobases] WHERE [Guid] = @v1", objConn);
                command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Char)).Value = InfobaseGuid;

                SqlDataReader rs = command.ExecuteReader();

                if (rs.Read())
                {
                    InfobaseID = Convert.ToInt32(rs[0]);
                }

                rs.Close();

                if (InfobaseID == 0)
                {
                    command.CommandText = @"INSERT INTO Infobases ([Code], [Name], [guid])
                                        SELECT MAX(f) AS[Code],
                                               @v0 AS[Name],
                                               @v1 AS[guid]
                                        FROM(SELECT MAX(Code) + 1 AS f
                                              FROM Infobases
                                              UNION ALL
                                              SELECT 1 AS Expr1) AS T; ";

                    command.Parameters.Clear();
                    command.Parameters.Add(new SqlParameter("@v0", SqlDbType.Char)).Value = InfobaseName;
                    command.Parameters.Add(new SqlParameter("@v1", SqlDbType.Char)).Value = InfobaseGuid;
                    command.ExecuteNonQuery();

                    command.CommandText = "SELECT [Code] FROM [dbo].[Infobases] WHERE [Guid] = @v1";
                    rs = command.ExecuteReader();

                    if (rs.Read())
                    {
                        InfobaseID = Convert.ToInt32(rs[0]);
                    }

                    rs.Close();
                }

                command.Dispose();
                objConn.Close();
                objConn.Dispose();
            }
            else if (ItIsMySQL)
            {
                MySqlConnection objConn = new MySqlConnection(ConnectionString);
                objConn.Open();

                MySqlCommand command = new MySqlCommand("SELECT `Code` FROM `Infobases` WHERE `Guid` = @v1", objConn);
                command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.VarChar)).Value = InfobaseGuid;

                MySqlDataReader rs = command.ExecuteReader();

                if (rs.Read())
                {
                    InfobaseID = Convert.ToInt32(rs[0]);
                }

                rs.Close();

                if (InfobaseID == 0)
                {
                    command.CommandText = "INSERT INTO Infobases (`Code`,`Name`,`guid`)" +
                                     " SELECT MAX(f) AS `Code`, @v0 as `Name`, @v1 as `guid` FROM " +
                                     " (SELECT MAX(Code) + 1 AS f FROM `Infobases` UNION ALL" +
                                     " SELECT 1 AS `Expr1`) AS T";

                    command.Parameters.Clear();
                    command.Parameters.Add(new MySqlParameter("@v0", MySqlDbType.VarChar)).Value = InfobaseName;
                    command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.VarChar)).Value = InfobaseGuid;
                    command.ExecuteNonQuery();

                    command.CommandText = "SELECT `Code` FROM `Infobases` WHERE `Guid` = @v1";

                    rs = command.ExecuteReader();

                    if (rs.Read())
                    {
                        InfobaseID = Convert.ToInt32(rs[0]);
                    }

                    rs.Close();
                }

                command.Dispose();
                objConn.Close();
                objConn.Dispose();
            }
        }


        public void SaveReadParametersToFile()
        {
            string readParametersFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"read-setting-{InfobaseGuid}.json");

            ReadParameters ConfigSettingObj = new ReadParameters
            {
                CurrentPosition = this.CurrentPosition,
                CurrentFilename = this.CurrentFilename,
                LastEventNumber83 = this.LastEventNumber83
            };

            string JsonText = JsonConvert.SerializeObject(ConfigSettingObj, Formatting.Indented);

            //Console.WriteLine(readParametersFile);
            //Console.WriteLine(JsonText);

            File.WriteAllText(readParametersFile, JsonText);
        }


        public void GetReadParametersFromFile()
        {
            string ReadParametersFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"read-setting-{InfobaseGuid}.json");

            if (File.Exists(ReadParametersFile))
            {
                string JsonText = File.ReadAllText(ReadParametersFile);

                ReadParameters ConfigSettingObj = JsonConvert.DeserializeObject<ReadParameters>(JsonText);

                this.CurrentPosition = Convert.ToInt64(ConfigSettingObj.CurrentPosition);
                this.CurrentFilename = ConfigSettingObj.CurrentFilename;
                this.LastEventNumber83 = ConfigSettingObj.LastEventNumber83;
            }
        }


        public void SaveEventsToSQL()
        {
            if (EventsList.Count == 0)
                return;

            if (ItIsMSSQL)
            {
                SqlConnection objConn = new SqlConnection(ConnectionString);
                objConn.Open();

                DataTable dt = new DataTable();

                /*
	            [InfobaseCode] [int] NOT NULL,
	            [DateTime] [datetime] NOT NULL,
	            [TransactionStatus] [varchar](1) NULL,
	            [TransactionStartTime] [datetime] NULL,
	            [TransactionMark] [bigint] NULL,
	            [Transaction] [varchar](100) NULL,
	            [UserName] [int] NULL,
	            [ComputerName] [int] NULL,
	            [AppName] [int] NULL,
	            [EventID] [int] NULL,
	            [EventType] [varchar](1) NULL,
	            [Comment] [nvarchar](max) NULL,
	            [MetadataID] [int] NULL,
	            [DataStructure] [nvarchar](max) NULL,
	            [DataString] [nvarchar](max) NULL,
	            [ServerID] [int] NULL,
	            [MainPortID] [int] NULL,
	            [SecondPortID] [int] NULL,
	            [Seance] [int] NULL
                 */

                dt.Columns.Add(new DataColumn("InfobaseCode", typeof(int)));
                dt.Columns.Add(new DataColumn("DateTime", typeof(DateTime)));
                dt.Columns.Add(new DataColumn("TransactionStatus", typeof(string)));
                dt.Columns.Add(new DataColumn("TransactionStartTime", typeof(DateTime)));
                dt.Columns.Add(new DataColumn("TransactionMark", typeof(long)));
                dt.Columns.Add(new DataColumn("Transaction", typeof(string)));
                dt.Columns.Add(new DataColumn("UserName", typeof(int)));
                dt.Columns.Add(new DataColumn("ComputerName", typeof(int)));
                dt.Columns.Add(new DataColumn("AppName", typeof(int)));
                dt.Columns.Add(new DataColumn("EventID", typeof(int)));
                dt.Columns.Add(new DataColumn("EventType", typeof(string)));
                dt.Columns.Add(new DataColumn("Comment", typeof(string)));
                dt.Columns.Add(new DataColumn("MetadataID", typeof(int)));
                dt.Columns.Add(new DataColumn("DataStructure", typeof(string)));
                dt.Columns.Add(new DataColumn("DataString", typeof(string)));
                dt.Columns.Add(new DataColumn("ServerID", typeof(int)));
                dt.Columns.Add(new DataColumn("MainPortID", typeof(int)));
                dt.Columns.Add(new DataColumn("SecondPortID", typeof(int)));
                dt.Columns.Add(new DataColumn("Seance", typeof(int)));

                foreach (OneEventRecord eventRecord in EventsList)
                {
                    if (eventRecord.AppName == 0)
                        continue;

                    DataRow row = dt.NewRow();

                    try
                    {
                        row[0] = InfobaseID;
                        row[1] = eventRecord.DateTime;
                        row[2] = eventRecord.TransactionStatus;
                        row[3] = eventRecord.TransactionStartTime;
                        row[4] = eventRecord.TransactionMark;
                        row[5] = eventRecord.Transaction;
                        row[6] = eventRecord.UserName;
                        row[7] = eventRecord.ComputerName;
                        row[8] = eventRecord.AppName;
                        row[9] = eventRecord.EventID;
                        row[10] = eventRecord.EventType;
                        row[11] = eventRecord.Comment;
                        row[12] = eventRecord.MetadataID;
                        row[13] = eventRecord.DataStructure;
                        row[14] = eventRecord.DataString;
                        row[15] = eventRecord.ServerID;
                        row[16] = eventRecord.MainPortID;
                        row[17] = eventRecord.SecondPortID;
                        row[18] = eventRecord.SessionNumber;

                        dt.Rows.Add(row);
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, this.GetType().ToString());
                    }
                }

                

                using (SqlBulkCopy copy = new SqlBulkCopy(objConn))
                {
                    for (int jj = 0; jj <= 18; jj++)
                    {
                        copy.ColumnMappings.Add(jj, jj);
                    }

                    copy.DestinationTableName = "Events";
                    copy.WriteToServer(dt);
                }

                SaveReadParametersToFile();

                Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff",
                                            CultureInfo.InvariantCulture) + $" New records have been processed: {EventsList.Count.ToString()} ({InfobaseName})");

                objConn.Close();
                objConn.Dispose();
            }
            else if (ItIsMySQL)
            {
                MySqlConnection objConn = new MySqlConnection(ConnectionString);
                objConn.Open();

                MySqlCommand command = new MySqlCommand("START TRANSACTION", objConn);
                command.ExecuteNonQuery();

                command.CommandText = "INSERT INTO `Events` (`InfobaseCode`,`DateTime`,`TransactionStatus`,`Transaction`,`UserName`,`ComputerName`" +
                                          ",`AppName`,`EventID`,`EventType`,`Comment`,`MetadataID`,`DataStructure`,`DataString`" +
                                          ",`ServerID`,`MainPortID`,`SecondPortID`,`Seance`,`TransactionStartTime`,`TransactionMark`)" +
                                          " VALUES(@v0,@v1,@v2,@v3,@v4,@v5,@v6,@v7,@v8,@v9,@v10,@v11,@v12,@v13,@v14,@v15,@v16,@v17,@v18)";

                int i = 0;

                foreach (OneEventRecord eventRecord in EventsList)
                {
                    try
                    {
                        command.Parameters.Clear();
                        command.Parameters.Add(new MySqlParameter("@v0", MySqlDbType.Int32)).Value = InfobaseID;
                        command.Parameters.Add(new MySqlParameter("@v1", MySqlDbType.DateTime)).Value = eventRecord.DateTime;
                        command.Parameters.Add(new MySqlParameter("@v2", MySqlDbType.VarChar)).Value = eventRecord.TransactionStatus;
                        command.Parameters.Add(new MySqlParameter("@v3", MySqlDbType.VarChar)).Value = eventRecord.Transaction;
                        command.Parameters.Add(new MySqlParameter("@v4", MySqlDbType.Int32)).Value = eventRecord.UserName;
                        command.Parameters.Add(new MySqlParameter("@v5", MySqlDbType.Int32)).Value = eventRecord.ComputerName;
                        command.Parameters.Add(new MySqlParameter("@v6", MySqlDbType.Int32)).Value = eventRecord.AppName;
                        command.Parameters.Add(new MySqlParameter("@v7", MySqlDbType.Int32)).Value = eventRecord.EventID;
                        command.Parameters.Add(new MySqlParameter("@v8", MySqlDbType.VarChar)).Value = eventRecord.EventType;
                        command.Parameters.Add(new MySqlParameter("@v9", MySqlDbType.VarChar)).Value = eventRecord.Comment;
                        command.Parameters.Add(new MySqlParameter("@v10", MySqlDbType.Int32)).Value = eventRecord.MetadataID;
                        command.Parameters.Add(new MySqlParameter("@v11", MySqlDbType.VarChar)).Value = eventRecord.DataStructure;
                        command.Parameters.Add(new MySqlParameter("@v12", MySqlDbType.VarChar)).Value = eventRecord.DataString;
                        command.Parameters.Add(new MySqlParameter("@v13", MySqlDbType.Int32)).Value = eventRecord.ServerID;
                        command.Parameters.Add(new MySqlParameter("@v14", MySqlDbType.Int32)).Value = eventRecord.MainPortID;
                        command.Parameters.Add(new MySqlParameter("@v15", MySqlDbType.Int32)).Value = eventRecord.SecondPortID;
                        command.Parameters.Add(new MySqlParameter("@v16", MySqlDbType.Int32)).Value = eventRecord.SessionNumber;
                        command.Parameters.Add(new MySqlParameter("@v17", MySqlDbType.DateTime)).Value = eventRecord.TransactionStartTime;
                        command.Parameters.Add(new MySqlParameter("@v18", MySqlDbType.Int64)).Value = eventRecord.TransactionMark;

                        command.ExecuteNonQuery();

                        i += 1;
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"Ошибка сохранения в БД записи от {eventRecord.DateTime.ToString()} по ИБ {InfobaseName} : {ex.Message}");
                    }
                }

                Console.WriteLine($"{DateTime.Now.ToShortTimeString()} New records have been processed: {i.ToString()}");

                SaveReadParametersToFile();

                command.CommandText = "COMMIT";
                command.Parameters.Clear();
                command.ExecuteNonQuery();

                command.Dispose();
                objConn.Close();
                objConn.Dispose();
            }
            //else if (ItIsES)
            //{
            //    var node = new Uri(ConnectionString);

            //    var _settings = new ConnectionSettings(node).DefaultIndex(ESIndexName).MaximumRetries(2).MaxRetryTimeout(TimeSpan.FromSeconds(150));
            //    var _current = new ElasticClient(_settings);

            //    //'Let's create proper array for ES
            //    List<object> NewRecords = new List<object>();

            //    foreach (var EventRecord in EventsList)
            //    {
            //        ESRecord ESRecord = new ESRecord { ServerName = ESServerName, DatabaseName = InfobaseName };
            //        ESRecord.RowID = EventRecord.RowID;

            //        switch (EventRecord.Severity)
            //        {
            //            case 1:
            //                ESRecord.Severity = "Information";
            //                break;
            //            case 2:
            //                ESRecord.Severity = "Warning";
            //                break;
            //            case 3:
            //                ESRecord.Severity = "Error";
            //                break;
            //            case 4:
            //                ESRecord.Severity = "Note";
            //                break;
            //        }

            //        ESRecord.DateTime = EventRecord.DateTime;
            //        ESRecord.ConnectID = EventRecord.ConnectID;
            //        ESRecord.DataType = EventRecord.DataType;
            //        ESRecord.SessionNumber = EventRecord.SessionNumber;
            //        ESRecord.DataStructure = EventRecord.DataStructure;
            //        ESRecord.DataString = EventRecord.DataString;
            //        ESRecord.Comment = EventRecord.Comment;
            //        ESRecord.SessionDataSplitCode = EventRecord.SessionDataSplitCode;


            //        var EventObj = new EventType();
            //        if (DictEvents.TryGetValue(EventRecord.EventID, out EventObj))
            //        {
            //            ESRecord.EventType = EventObj;
            //        }

            //        var MetadataObj = new Metadata();
            //        if (DictMetadata.TryGetValue(EventRecord.MetadataID, out MetadataObj))
            //        {
            //            ESRecord.Metadata = MetadataObj;
            //        }

            //        var ComputerObj = new Computer();
            //        if (DictComputers.TryGetValue(EventRecord.ComputerName, out ComputerObj))
            //        {
            //            ESRecord.Computer = ComputerObj.Name;
            //        }

            //        var MainPortObj = new MainPort();
            //        if (DictMainPorts.TryGetValue(EventRecord.MainPortID, out MainPortObj))
            //        {
            //            ESRecord.PrimaryPort = MainPortObj.Name;
            //        }

            //        var ServerObj = new Server();
            //        if (DictServers.TryGetValue(EventRecord.ServerID, out ServerObj))
            //        {
            //            ESRecord.Server = ServerObj.Name;
            //        }

            //        var SecondPortObj = new SecondPort();
            //        if (DictSecondPorts.TryGetValue(EventRecord.SecondPortID, out SecondPortObj))
            //        {
            //            ESRecord.SecondaryPort = SecondPortObj.Name;
            //        }

            //        var ApplicationObj = new Application();
            //        if (DictApplications.TryGetValue(EventRecord.AppName, out ApplicationObj))
            //        {
            //            ESRecord.Application = ApplicationObj.Name;
            //        }

            //        var UserNameObj = new User();
            //        if (DictUsers.TryGetValue(EventRecord.UserName, out UserNameObj))
            //        {
            //            ESRecord.UserName = UserNameObj;
            //        }

            //        if (ESUseSynonymsForFieldsNames)
            //        {
            //            var ESRecordUserFields = new Dictionary<string, object>();
            //            ESRecordUserFields.Add(ESFieldSynonyms.ServerName, ESRecord.ServerName);
            //            ESRecordUserFields.Add(ESFieldSynonyms.DatabaseName, ESRecord.DatabaseName);
            //            ESRecordUserFields.Add(ESFieldSynonyms.RowID, ESRecord.RowID);
            //            ESRecordUserFields.Add(ESFieldSynonyms.Severity, ESRecord.Severity);
            //            ESRecordUserFields.Add(ESFieldSynonyms.DateTime, ESRecord.DateTime);
            //            ESRecordUserFields.Add(ESFieldSynonyms.ConnectID, ESRecord.ConnectID);
            //            ESRecordUserFields.Add(ESFieldSynonyms.DataType, ESRecord.DataType);
            //            ESRecordUserFields.Add(ESFieldSynonyms.SessionNumber, ESRecord.SessionNumber);
            //            ESRecordUserFields.Add(ESFieldSynonyms.DataStructure, ESRecord.DataStructure);
            //            ESRecordUserFields.Add(ESFieldSynonyms.DataString, ESRecord.DataString);
            //            ESRecordUserFields.Add(ESFieldSynonyms.Comment, ESRecord.Comment);
            //            ESRecordUserFields.Add(ESFieldSynonyms.SessionDataSplitCode, ESRecord.SessionDataSplitCode);
            //            ESRecordUserFields.Add(ESFieldSynonyms.EventType, ESRecord.EventType);
            //            ESRecordUserFields.Add(ESFieldSynonyms.Metadata, ESRecord.Metadata);
            //            ESRecordUserFields.Add(ESFieldSynonyms.Computer, ESRecord.Computer);
            //            ESRecordUserFields.Add(ESFieldSynonyms.PrimaryPort, ESRecord.PrimaryPort);
            //            ESRecordUserFields.Add(ESFieldSynonyms.Server, ESRecord.Server);
            //            ESRecordUserFields.Add(ESFieldSynonyms.SecondaryPort, ESRecord.SecondaryPort);
            //            ESRecordUserFields.Add(ESFieldSynonyms.Application, ESRecord.Application);
            //            ESRecordUserFields.Add(ESFieldSynonyms.UserName, ESRecord.UserName);

            //            NewRecords.Add(ESRecordUserFields);
            //        }
            //        else
            //        {
            //            NewRecords.Add(ESRecord);
            //        }
            //    }

            //    var Result = _current.IndexMany(NewRecords, ESIndexName, "event-log-record");

            //    Console.WriteLine(DateTime.Now.ToShortTimeString() + " New records have been processed " + NewRecords.Count.ToString());

            //    SaveReadParametersToFile();
            //}

            EventsList.Clear();
        }


        public void LoadReferenceFromTheTextFile(string FileName, ref string LastProcessedObjectForDebug)
        {
            FileStream FS = new FileStream(FileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            StreamReader SR = new StreamReader(FS);

            string Text = SR.ReadToEnd();

            SR.Close();
            FS.Close();

            Text = Text.Substring(Text.IndexOf("{"));

            List<string> ObjectTexts = ParserServices.ParseEventLogString("{" + Text + "}");

            foreach (string textObject in ObjectTexts)
            {
                LastProcessedObjectForDebug = textObject;

                List<string> a = ParserServices.ParseEventLogString(textObject);

                if (a != null)
                {
                    switch (a[0])
                    {
                        case "1":
                            AddUser(Convert.ToInt32(a[3]), a[1], a[2]);
                            break;

                        case "2":
                            AddComputer(Convert.ToInt32(a[2]), a[1]);
                            break;

                        case "3":
                            AddApplication(Convert.ToInt32(a[2]), a[1]);
                            break;

                        case "4":
                            AddEvent(Convert.ToInt32(a[2]), a[1]);
                            break;

                        case "5":
                            AddMetadata(Convert.ToInt32(a[3]), a[1], a[2]);
                            break;

                        case "6":
                            AddServer(Convert.ToInt32(a[2]), a[1]);
                            break;

                        case "7":
                            AddMainPort(Convert.ToInt32(a[2]), a[1]);
                            break;

                        case "8":
                            AddSecondPort(Convert.ToInt32(a[2]), a[1]);
                            break;

                        //'Case "9" - не видел этих в файле
                        //'Case "10"
                        case "11":
                        case "12":
                        case "13":
                            //'в числе последних трех должны быть статус транзакции и важность
                            break;
                    }
                }
            }

            SaveReferenceValuesToDatabase();
        }


        public void LoadReference()
        {
            //'Clear all reference dictionaries
            UsersDictionary.Clear();
            ComputersDictionary.Clear();
            ApplicationsDictionary.Clear();
            EventsDictionary.Clear();
            MetadataDictionary.Clear();
            ServersDictionary.Clear();
            MainPortsDictionary.Clear();
            SecondPortsDictionary.Clear();

            string fileName = Path.Combine(Catalog, "1Cv8.lgd");

            if (File.Exists(fileName))
            {
                try
                {
                    SQLiteConnection connection = new SQLiteConnection("Data Source=" + fileName);
                    connection.Open();

                    SQLiteCommand command = new SQLiteCommand
                    {
                        Connection = connection,
                        CommandText = "SELECT [code], [name] FROM [AppCodes]"
                    };

                    SQLiteDataReader rs = command.ExecuteReader();

                    while (rs.Read())
                    {
                        AddApplication((long)rs[0], ((string)rs[1]).RemoveQuotes());
                    }

                    rs.Close();

                    command.CommandText = "SELECT [code], [name] FROM [ComputerCodes]";
                    rs = command.ExecuteReader();

                    while (rs.Read())
                    {
                        AddComputer((long)rs[0], ((string)rs[1]).RemoveQuotes());
                    }

                    rs.Close();

                    command.CommandText = "SELECT [code], [name] FROM [EventCodes]";
                    rs = command.ExecuteReader();

                    while (rs.Read())
                    {
                        AddEvent((long)rs[0], ((string)rs[1]).RemoveQuotes());
                    }

                    rs.Close();

                    command.CommandText = "SELECT [code], [uuid], [name] FROM [UserCodes]";
                    rs = command.ExecuteReader();

                    while (rs.Read())
                    {
                        AddUser((long)rs[0], (string)rs[1], (string)rs[2]);
                    }

                    rs.Close();

                    command.CommandText = "SELECT [code], [name] FROM [WorkServerCodes]";
                    rs = command.ExecuteReader();

                    while (rs.Read())
                    {
                        AddServer((long)rs[0], ((string)rs[1]).RemoveQuotes());
                    }

                    rs.Close();

                    command.CommandText = "SELECT [code], [uuid], [name] FROM [MetadataCodes]";
                    rs = command.ExecuteReader();

                    while (rs.Read())
                    {
                        AddMetadata((long)rs[0], (string)rs[1], (string)rs[2]);
                    }

                    rs.Close();

                    command.CommandText = "SELECT [code], [name] FROM [PrimaryPortCodes]";
                    rs = command.ExecuteReader();

                    while (rs.Read())
                    {
                        AddMainPort((long)rs[0], (string)rs[1]);
                    }

                    rs.Close();

                    command.CommandText = "SELECT [code], [name] FROM [SecondaryPortCodes]";
                    rs = command.ExecuteReader();

                    while (rs.Read())
                    {
                        AddSecondPort((long)rs[0], (string)rs[1]);
                    }

                    rs.Close();

                    command.Dispose();
                    connection.Close();
                    connection.Dispose();

                    SaveReferenceValuesToDatabase();
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error occurred while working with reference tables");
                }
            }
            else
            {
                string lastProcessedObjectForDebug = string.Empty;
                fileName = Path.Combine(Catalog, "1Cv8.lgf");

                if (File.Exists(fileName))
                {
                    try
                    {
                        FileInfo FI = new FileInfo(fileName);

                        if (FI.LastWriteTime >= LastReferenceUpdate)
                        {
                            LoadReferenceFromTheTextFile(fileName, ref lastProcessedObjectForDebug);
                        }
                    }
                    catch (Exception ex)
                    {
                        string AdditionalString = string.Empty;

                        if (!string.IsNullOrEmpty(lastProcessedObjectForDebug))
                        {
                            AdditionalString = $"Attempted to process this object: {lastProcessedObjectForDebug}";
                        }

                        Log.Error(ex, $"Error occurred while working with reference file. {AdditionalString}");
                    }
                }
            }
        }


        public void FindAndStartParseFiles()
        {
            string v83File = Path.Combine(Catalog, "1Cv8.lgd");

            if (File.Exists(v83File))
            {
                LoadEvents83(v83File);

                SaveEventsToSQL();
            }
            else
            {
                List<string> ArrayFiles = new List<string>();

                if (Directory.Exists(Catalog))
                {
                    string[] files = Directory.GetFiles(Catalog);

                    foreach (string file in files)
                    {
                        if (file.EndsWith(".lgp"))
                        {
                            ArrayFiles.Add(file);
                        }
                    }

                    ArrayFiles.Sort();

                    foreach (string file in ArrayFiles)
                    {
                        if (file != null)
                        {
                            try
                            {
                                FileInfo fileInfo = new FileInfo(file);

                                if (string.Compare(fileInfo.Name, CurrentFilename) >= 0)
                                {
                                    if (!(fileInfo.Name == CurrentFilename))
                                    {
                                        CurrentPosition = 2;// ' start position for log-file 1C
                                    }

                                    CurrentFilename = fileInfo.Name;

                                    LoadEvents2(file);
                                }
                                else if (string.Compare(fileInfo.Name, CurrentFilename) < 0)
                                {
                                    //File.Delete(file);
                                }
                            }
                            catch (Exception ex)
                            {
                                Log.Error(ex, "Error in FindAndStartParseFiles");
                            }
                        }
                    }

                    SaveEventsToSQL();
                }
            }
        }


        public void LoadEvents83(string fileName)
        {
            try
            {
                SQLiteConnection connection = new SQLiteConnection($"Data Source={fileName}");
                connection.Open();

                SQLiteCommand command = new SQLiteCommand
                {
                    Connection = connection
                };

                while (true)
                {
                    command.CommandText = @"SELECT 
                                            [rowID],
                                            [severity],
                                            [date],
                                            [connectID],
                                            [session],
                                            [transactionStatus],
                                            [transactionDate],
                                            [transactionID],
                                            [userCode],
                                            [computerCode],
                                            [appCode],
                                            [eventCode],
                                            [comment],
                                            [metadataCodes],
                                            [sessionDataSplitCode],
                                            [dataType],
                                            [data],
                                            [dataPresentation],
                                            [workServerCode],
                                            [primaryPortCode],
                                            [secondaryPortCode]
                                        FROM[EventLog]
                                        WHERE[rowID] > @LastEventNumber83 AND[date] >= @MinimumDate
                                      ORDER BY 1
                                        LIMIT 10";

                    command.Parameters.AddWithValue("LastEventNumber83", LastEventNumber83);

                    double noOfSeconds = (LoadEventsStartingAt - new DateTime()).TotalSeconds;
                    command.Parameters.AddWithValue("MinimumDate", noOfSeconds * 10000);

                    SQLiteDataReader reader = command.ExecuteReader();

                    bool hasData = reader.HasRows;

                    while (reader.Read())
                    {
                        OneEventRecord eventRecord = new OneEventRecord(reader);

                        EventsList.Add(eventRecord);

                        this.LastEventNumber83 = eventRecord.RowID;
                    }

                    reader.Close();

                    if (EventsList.Count > 0)
                    {
                        SaveEventsToSQL();
                    }

                    if (!hasData)
                        break;
                }

                command.Dispose();
                connection.Close();
                connection.Dispose();
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error while working with EventLog table (SQLite)");
            }
        }


        public void LoadEvents2(string FileName)
        {
            File.Copy(FileName, Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"logCopy_{InfobaseGuid.ToString()}.log"), true);

            TextReader reader = File.OpenText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"logCopy_{InfobaseGuid.ToString()}.log"));
            //string[] fileLines = reader.ReadToEnd().Split(new string[] { Environment.NewLine }, StringSplitOptions.None);

            string eventString = string.Empty;

            //пропуск уже загруженных строк
            long lineNumber;
            string newline = string.Empty;
            for (lineNumber = 1; lineNumber <= this.CurrentPosition; lineNumber++)
            {
                newline = reader.ReadLine();
            }

            //загрузка новых строк
            while (newline != null)
            {
                eventString += newline;

                if (newline == "}" || newline == "},")
                {
                    try
                    {
                        this.CurrentPosition = lineNumber + 1;

                        AddEvent(eventString);
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, this.GetType().ToString());
                    }
                    
                    eventString = string.Empty;
                }

                newline = reader.ReadLine();
                lineNumber++;
            }

            reader.Close();
        }


        public void AddEvent(string eventString)
        {
            OneEventRecord eventRecord = new OneEventRecord(eventString);

            if (eventRecord.DateTime < LoadEventsStartingAt)
                return;

            EventsList.Add(eventRecord);

            if (EventsList.Count >= 10)
            {
                SaveEventsToSQL();
            }
        }


        public void DoWork()
        {
            while (true)
            {
                Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)} Start new iteration... ({InfobaseName})");

                try
                {
                    LoadReference();

                    GetReadParametersFromFile();

                    FindAndStartParseFiles();
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"Error occurred during log file processing ({InfobaseName})");
                }

                Thread.Sleep(SleepTime);
            }
        }
    }
}
