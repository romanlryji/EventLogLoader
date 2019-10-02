using System;

namespace EventLogApp
{
    internal class ESRecord
    {
        public long RowID { get; set; }
        public string ServerName { get; set; }
        public string DatabaseName { get; set; }
        public DateTime DateTime { get; set; }
        public string Severity { get; set; }
        public EventType EventType { get; set; }
        public string Computer { get; set; }
        public string Application { get; set; }
        public Metadata Metadata { get; set; }
        public User UserName { get; set; }
        public int SessionNumber { get; set; }
        public int ConnectID { get; set; }
        public int DataType { get; set; }
        public string DataStructure { get; set; }
        public string DataString { get; set; }
        public string Comment { get; set; }
        public string PrimaryPort { get; set; }
        public string SecondaryPort { get; set; }
        public string Server { get; set; }
        public int SessionDataSplitCode { get; set; }
    }
}
