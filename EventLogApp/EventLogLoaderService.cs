using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Threading;

namespace EventLogApp
{
    public class EventLogLoaderService
    {
        List<EventLogProcessor> ListOfProcessors = new List<EventLogProcessor>();
        List<Thread> ArrayThread;

        public ConfigSetting ConfigSettingObj = new ConfigSetting();
        string ConnectionString;
        string DBType;
        bool ItIsMySQL;
        bool ItIsMSSQL;
        bool ItIsElasticSearch;
        public int SleepTime = 60 * 1000; //1 мин

        public Thread WorkerThread;
        private NLog.Logger Log;

        public EventLogLoaderService()
        {
            //InitializeComponent();

            Directory.SetCurrentDirectory(AppDomain.CurrentDomain.BaseDirectory);

            Log = NLog.LogManager.GetLogger("CurrentThread");

            ArrayThread = new List<Thread>();

            WorkerThread = new Thread(DoWork);
            WorkerThread.SetApartmentState(ApartmentState.STA);
        }


        public void DoWork()
        {
            if (!LoadConfigSetting())
            {
                Log.Error("Error while working with config.json file in application folder");
                Environment.Exit(-1);
            }

            CreateTables();

            foreach (EventLogProcessor IB in ListOfProcessors)
            {
                try
                {
                    IB.GetInfobaseIDFromDatabase();
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"Error occurred while getting infobase ID from target database ({IB.InfobaseName})");

                    continue;
                }

                try
                {
                    var Thead = new Thread(IB.DoWork)
                    {
                        IsBackground = false
                    };

                    Thead.Start();

                    ArrayThread.Add(Thead);

                    //-------------------------------------

                    //IB.DoWork();
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"Error occurred while starting new thread ({IB.InfobaseName})");
                }
            }
        }


        public void CreateTables()
        {
            try
            {
                if (ItIsMSSQL)
                {
                    SqlConnection objConn = new SqlConnection(ConnectionString);
                    objConn.Open();

                    var command = new SqlCommand(@"IF NOT EXISTS (select * from sysobjects where id = object_id(N'Events'))
                                                BEGIN
                                                  CREATE TABLE[dbo].[Events]([InfobaseCode] int Not NULL, [DateTime][datetime] Not NULL,
                                                        [TransactionStatus][varchar](1) NULL,
                                                        [TransactionStartTime][datetime] NULL,

                                                          [TransactionMark] bigint NULL,
                                                        [Transaction][varchar](100) NULL,

                                                          [UserName] int NULL,

                                                          [ComputerName] int NULL,

                                                          [AppName] Int NULL,
                                                        [EventID] int NULL,

                                                          [EventType][varchar](1) NULL,
                                                        [Comment][nvarchar](max) NULL,

                                                          [MetadataID] int NULL,

                                                          [DataStructure][nvarchar](max) NULL,

                                                          [DataString][nvarchar](max) NULL,
                                                        [ServerID] int NULL,

                                                          [MainPortID] int NULL,
                                                        [SecondPortID] int NULL,

                                                          [Seance] int NULL);
                    CREATE CLUSTERED INDEX[CIX_Events] ON[dbo].[Events]([InfobaseCode], [DateTime])
                                                END", objConn);
                    command.ExecuteNonQuery();

                    command.CommandText = "IF NOT EXISTS (select * from sysobjects where id = object_id(N'Infobases'))" + Environment.NewLine +
                                    "	CREATE TABLE [dbo].[Infobases] ([Guid] [char](40) NOT NULL, [Code] int NOT NULL, [Name] [char](100))" + Environment.NewLine +
                                    " IF NOT EXISTS (select * from sys.indexes where object_id = object_id(N'Infobases') AND Name = 'ClusteredIndex')" + Environment.NewLine +
                                    " CREATE UNIQUE CLUSTERED INDEX [ClusteredIndex] ON [dbo].[Infobases] ([Guid] ASC);";
                    command.ExecuteNonQuery();

                    command.CommandText =
                    "IF NOT EXISTS (select * from sysobjects where id = object_id(N'Users'))" + Environment.NewLine +
                    "CREATE TABLE [dbo].[Users]([InfobaseCode] int NOT NULL, [Code] int NOT NULL, [Name] [nvarchar](100), [Guid] [varchar](40));" + Environment.NewLine +
                    "IF NOT EXISTS (select * from sys.indexes where object_id = object_id(N'Users') AND Name = 'ClusteredIndex')" + Environment.NewLine +
                    "CREATE UNIQUE CLUSTERED INDEX [ClusteredIndex] ON [dbo].[Users] ([InfobaseCode] ASC, [Code] ASC);" + Environment.NewLine +
                    "" +
                    "IF NOT EXISTS (select * from sysobjects where id = object_id(N'Metadata'))" + Environment.NewLine +
                    "CREATE TABLE [dbo].[Metadata]([InfobaseCode] int NOT NULL, [Code] int NOT NULL, [Name] [nvarchar](100), [Guid] [varchar](40));" + Environment.NewLine +
                    "IF NOT EXISTS (select * from sys.indexes where object_id = object_id(N'Metadata') AND Name = 'ClusteredIndex')" + Environment.NewLine +
                    "CREATE UNIQUE CLUSTERED INDEX [ClusteredIndex] ON [dbo].[Metadata] ([InfobaseCode] ASC, [Code] ASC);" + Environment.NewLine +
                    "" +
                    "IF NOT EXISTS (select * from sysobjects where id = object_id(N'Computers'))" + Environment.NewLine +
                    "CREATE TABLE [dbo].[Computers]([InfobaseCode] int NOT NULL, [Code] int NOT NULL, [Name] [nvarchar](100));" + Environment.NewLine +
                    "IF NOT EXISTS (select * from sys.indexes where object_id = object_id(N'Computers') AND Name = 'ClusteredIndex')" + Environment.NewLine +
                    "CREATE UNIQUE CLUSTERED INDEX [ClusteredIndex] ON [dbo].[Computers] ([InfobaseCode] ASC, [Code] ASC);" + Environment.NewLine +
                    "" +
                    "IF NOT EXISTS (select * from sysobjects where id = object_id(N'Applications'))" + Environment.NewLine +
                    "CREATE TABLE [dbo].[Applications]([InfobaseCode] int NOT NULL, [Code] int NOT NULL,[Name] [nvarchar](100));" + Environment.NewLine +
                    "IF NOT EXISTS (select * from sys.indexes where object_id = object_id(N'Applications') AND Name = 'ClusteredIndex')" + Environment.NewLine +
                    "CREATE UNIQUE CLUSTERED INDEX [ClusteredIndex] ON [dbo].[Applications] ([InfobaseCode] ASC, [Code] ASC);" + Environment.NewLine +
                    "" +
                    "IF NOT EXISTS (select * from sysobjects where id = object_id(N'EventsType'))" + Environment.NewLine +
                    "CREATE TABLE [dbo].[EventsType]([InfobaseCode] int NOT NULL, [Code] int NOT NULL, [Name] [nvarchar](max));" + Environment.NewLine +
                    "IF NOT EXISTS (select * from sys.indexes where object_id = object_id(N'EventsType') AND Name = 'ClusteredIndex')" + Environment.NewLine +
                    "CREATE UNIQUE CLUSTERED INDEX [ClusteredIndex] ON [dbo].[EventsType] ([InfobaseCode] ASC, [Code] ASC);" + Environment.NewLine +
                    "" +
                    "IF NOT EXISTS (select * from sysobjects where id = object_id(N'Servers'))" + Environment.NewLine +
                    "CREATE TABLE [dbo].[Servers]([InfobaseCode] int NOT NULL, [Code] int NOT NULL, [Name] [nvarchar](100));" + Environment.NewLine +
                    "IF NOT EXISTS (select * from sys.indexes where object_id = object_id(N'Servers') AND Name = 'ClusteredIndex')" + Environment.NewLine +
                    "CREATE UNIQUE CLUSTERED INDEX [ClusteredIndex] ON [dbo].[Servers] ([InfobaseCode] ASC, [Code] ASC);" + Environment.NewLine +
                    "" +
                    "IF NOT EXISTS (select * from sysobjects where id = object_id(N'MainPorts'))" + Environment.NewLine +
                    "CREATE TABLE [dbo].[MainPorts]([InfobaseCode] int NOT NULL, [Code] int NOT NULL, [Name] [nvarchar](100));" + Environment.NewLine +
                    "IF NOT EXISTS (select * from sys.indexes where object_id = object_id(N'MainPorts') AND Name = 'ClusteredIndex')" + Environment.NewLine +
                    "CREATE UNIQUE CLUSTERED INDEX [ClusteredIndex] ON [dbo].[MainPorts] ([InfobaseCode] ASC, [Code] ASC);" + Environment.NewLine +
                    "" +
                    "IF NOT EXISTS (select * from sysobjects where id = object_id(N'SecondPorts'))" + Environment.NewLine +
                    "CREATE TABLE [dbo].[SecondPorts]([InfobaseCode] int NOT NULL, [Code] int NOT NULL, [Name] [nvarchar](100));" + Environment.NewLine +
                    "IF NOT EXISTS (select * from sys.indexes where object_id = object_id(N'SecondPorts') AND Name = 'ClusteredIndex')" + Environment.NewLine +
                    "CREATE UNIQUE CLUSTERED INDEX [ClusteredIndex] ON [dbo].[SecondPorts] ([InfobaseCode] ASC, [Code] ASC);";

                    command.ExecuteNonQuery();

                    command.CommandText = "SELECT TOP 1 * FROM Events";
                    command.ExecuteReader();

                    command.Dispose();
                    objConn.Close();
                    objConn.Dispose();
                }
                else if (ItIsMySQL)
                {
                    MySql.Data.MySqlClient.MySqlConnection objConn = new MySql.Data.MySqlClient.MySqlConnection(ConnectionString);
                    objConn.Open();

                    string DBName = objConn.Database;

                    MySql.Data.MySqlClient.MySqlCommand command = new MySql.Data.MySqlClient.MySqlCommand
                    {
                        Connection = objConn,
                        CommandText = "CREATE TABLE IF NOT EXISTS `Events` (`InfobaseCode` int(11) NOT NULL, `DateTime` int(11) NOT NULL," +
                    "`TransactionStatus` varchar(1) NULL, `TransactionStartTime` datetime NULL,	" +
                    "`TransactionMark` bigint NULL, `Transaction` varchar(100) NULL,	`UserName` int(11) NULL, `ComputerName` int(11) NULL,	" +
                    "`AppName` int(11) NULL, `EventID` int(11) NULL, `EventType` varchar(1) NULL,	" +
                    "`Comment` text NULL, `MetadataID` int(11) NULL,	`DataStructure` text NULL, `DataString` text NULL,	" +
                    "`ServerID` int(11) NULL, `MainPortID` int(11) NULL,	`SecondPortID` int(11) NULL, `Seance` int(11) NULL" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
                    };

                    command.ExecuteNonQuery();

                    command.CommandText = "CREATE TABLE IF NOT EXISTS `Infobases` (`Guid` varchar(40) NOT NULL, `Code` int(11) NOT NULL, `Name` varchar(100)," +
                                        "PRIMARY KEY `Guid` (`Guid`));";
                    command.ExecuteNonQuery();

                    command.CommandText =
                    "CREATE TABLE IF NOT EXISTS `Users`(`InfobaseCode` int(11) NOT NULL, `Code` int(11) NOT NULL, `Name` varchar(100), `Guid` varchar(40), PRIMARY KEY (`InfobaseCode`, `Code`));" + Environment.NewLine +
                    "" +
                    "CREATE TABLE IF NOT EXISTS `Metadata`(`InfobaseCode` int(11) NOT NULL, `Code` int(11) NOT NULL, `Name` varchar(100), `Guid` varchar(40), PRIMARY KEY (`InfobaseCode`, `Code`));" + Environment.NewLine +
                    "" +
                    "CREATE TABLE IF NOT EXISTS `Computers`(`InfobaseCode` int(11) NOT NULL, `Code` int(11) NOT NULL, `Name` varchar(100), PRIMARY KEY (`InfobaseCode`, `Code`));" + Environment.NewLine +
                    "" +
                    "CREATE TABLE IF NOT EXISTS `Applications`(`InfobaseCode` int(11) NOT NULL, `Code` int(11) NOT NULL, `Name` varchar(100), PRIMARY KEY (`InfobaseCode`, `Code`));" + Environment.NewLine +
                    "" +
                    "CREATE TABLE IF NOT EXISTS `EventsType`(`InfobaseCode` int(11) NOT NULL, `Code` int(11) NOT NULL, `Name` text, PRIMARY KEY (`InfobaseCode`, `Code`));" + Environment.NewLine +
                    "" +
                    "CREATE TABLE IF NOT EXISTS `Servers`(`InfobaseCode` int(11) NOT NULL, `Code` int(11) NOT NULL, `Name` varchar(100), PRIMARY KEY (`InfobaseCode`, `Code`));" + Environment.NewLine +
                    "" +
                    "CREATE TABLE IF NOT EXISTS `MainPorts`(`InfobaseCode` int(11) NOT NULL, `Code` int(11) NOT NULL, `Name` varchar(100), PRIMARY KEY (`InfobaseCode`, `Code`));" + Environment.NewLine +
                    "" +
                    "CREATE TABLE IF NOT EXISTS `SecondPorts`(`InfobaseCode` int(11) NOT NULL, `Code` int(11) NOT NULL, `Name` varchar(100), PRIMARY KEY (`InfobaseCode`, `Code`));";

                    command.ExecuteNonQuery();

                    command.Dispose();
                    objConn.Close();
                    objConn.Dispose();
                }

                Log.Info("Target database tables have been verified!");
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error occurred while during target database tables verification");
            }
        }


        public bool LoadConfigSetting()
        {
            try
            {
                string PathConfigFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config.json");

                if (File.Exists(PathConfigFile))
                {
                    ConfigSettingObj = ConfigSettings.LoadConfigSettingFromFile(PathConfigFile);

                    ConnectionString = ConfigSettingObj.ConnectionString;
                    DBType = ConfigSettingObj.DBType;

                    if (DBType == "MySQL")
                    {
                        ItIsMySQL = true;
                    }
                    else if (DBType == "MS SQL Server")
                    {
                        ItIsMSSQL = true;
                    }
                    else if (DBType == "ElasticSearch")
                    {
                        ItIsElasticSearch = true;
                    }

                    this.SleepTime = this.ConfigSettingObj.RepeatTime * 1000;

                    foreach (InfobaseSetting IBConfig in ConfigSettingObj.Infobases)
                    {
                        string IB_ID = IBConfig.DatabaseID;
                        string IB_Name = IBConfig.DatabaseName;
                        string IB_Catalog = IBConfig.DatabaseCatalog;

                        EventLogProcessor EventLogProcessorObj = new EventLogProcessor
                        {
                            Log = NLog.LogManager.GetLogger("CurrentThread"),
                            InfobaseGuid = IB_ID,
                            InfobaseName = IB_Name,
                            Catalog = IB_Catalog,
                            ConnectionString = this.ConnectionString,
                            SleepTime = this.SleepTime,
                            ItIsMSSQL = this.ItIsMSSQL,
                            ItIsMySQL = this.ItIsMySQL,
                            ItIsElasticSearch = this.ItIsElasticSearch,
                            ESIndexName = this.ConfigSettingObj.ESIndexName,
                            ESServerName = IBConfig.ESServerName,
                            ESUseSynonymsForFieldsNames = this.ConfigSettingObj.ESUseSynonymsForFieldsNames,
                            ESFieldSynonyms = this.ConfigSettingObj.ESFieldSynonyms
                        };

                        try
                        {
                            EventLogProcessorObj.LoadEventsStartingAt = DateTime.Parse(IBConfig.StartDate);
                        }
                        catch (Exception ex)
                        {
                            EventLogProcessorObj.LoadEventsStartingAt = new DateTime(1900, 1, 1);

                            Log.Error(ex, this.GetType().ToString());
                        }

                        ListOfProcessors.Add(EventLogProcessorObj);
                    }

                    return true;
                }
                else
                {
                    Log.Error("File config.json was not found!");

                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Parameters cannot be load from config.json file (it may be corrupted)");

                throw ex;
            }
        }


        public void SubStart()
        {
            WorkerThread.Start();
        }


        public void SubStop()
        {
            WorkerThread.Abort();
        }

    }
}
