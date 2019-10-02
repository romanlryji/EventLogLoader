using System.ServiceProcess;

namespace EventLogApp
{
    class Program
    {
        static void Main(string[] args)
        {
            EventLogLoaderService service = new EventLogLoaderService();

            service.DoWork();

            //------------------------------------------------------

            //ServiceBase[] ServicesToRun;

            //ServicesToRun = new ServiceBase[]
            //{
            //    new EventLogService()
            //};

            //ServiceBase.Run(ServicesToRun);
        }

    }
}
