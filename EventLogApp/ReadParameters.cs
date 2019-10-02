namespace EventLogApp
{
    internal class ReadParameters
    {
        public int InfobaseId { get; set; }
        public long CurrentPosition { get; set; }
        public string CurrentFilename { get; set; }
        public long LastEventNumber83 { get; set; }
    }
}
