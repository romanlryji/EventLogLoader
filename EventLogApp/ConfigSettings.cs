using Newtonsoft.Json;
using System.IO;

namespace EventLogApp
{
    class ConfigSettings
    {
        public static ConfigSetting LoadConfigSettingFromFile(string ConfigFilePath)
        {
            if (File.Exists(ConfigFilePath))
            {
                string JsonText = File.ReadAllText(ConfigFilePath);

                ConfigSetting ConfigSettingObj = JsonConvert.DeserializeObject<ConfigSetting>(JsonText);

                return ConfigSettingObj;
            }

            return new ConfigSetting();
        }


        public static void SaveConfigSettingToFile(ConfigSetting ConfigSettingObj, string ConfigFilePath)
        {
            string JsonText = JsonConvert.SerializeObject(ConfigSettingObj, Formatting.Indented);

            File.WriteAllText(ConfigFilePath, JsonText);
        }
    }
}
