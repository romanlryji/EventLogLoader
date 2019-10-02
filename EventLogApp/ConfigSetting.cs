using System.Collections.Generic;

namespace EventLogApp
{
    public class ConfigSetting
    {
        public string ConnectionString { get; set; }
        public string DBType { get; set; }
        public int RepeatTime { get; set; }
        public string ESIndexName { get; set; }
        public bool ESUseSynonymsForFieldsNames { get; set; }
        public ElasticSearchFieldSynonymsClass ESFieldSynonyms { get; set; }
        public List<InfobaseSetting> Infobases { get; set; }


        public ConfigSetting()
        {
            Infobases = new List<InfobaseSetting>();
            ESFieldSynonyms = new ElasticSearchFieldSynonymsClass();
        }
    }
}
