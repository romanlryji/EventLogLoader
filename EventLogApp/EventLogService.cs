using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace EventLogApp
{
    partial class EventLogService : ServiceBase
    {
        public EventLogService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            EventLogLoaderService service = new EventLogLoaderService();

            service.DoWork();
        }

        protected override void OnStop()
        {
            // TODO: Add code here to perform any tear-down necessary to stop your service.
        }
    }
}
