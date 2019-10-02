using System;
using System.Collections.Generic;

namespace EventLogApp
{
    class ParserServices
    {
        public static List<string> ParseEventLogString(string Text)
        {
            List<string> ArrayLines = new List<string>();

            var Text2 = Text.Substring(1, Text.EndsWith(",") ? Text.Length - 3 : Text.Length - 2) + ",";

            var Delim = Text2.IndexOf(",");

            string str = "";

            while (Delim > 0)
            {
                str = str + Text2.Substring(0, Delim).Trim();
                Text2 = Text2.Substring(Delim + 1);

                if (CountSubstringInString(str, "{") == CountSubstringInString(str, "}") && Math.IEEERemainder(CountSubstringInString(str, "\""), 2) == 0)
                {
                    if (str.StartsWith("\"") && str.EndsWith("\""))
                    {
                        str = str.Substring(1, str.Length - 2);
                    }

                    ArrayLines.Add(str);

                    str = "";
                }
                else
                {
                    str = str + ",";
                }

                Delim = Text2.IndexOf(",");
            }

            return ArrayLines;
        }


        public static int CountSubstringInString(string Str, string SubStr)
        {
            return (Str.Length - Str.Replace(SubStr, "").Length) / SubStr.Length;
        }
    }
}