import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import time
import subprocess

class SchedulerService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PythonSchedulerService"
    _svc_display_name_ = "Python Scheduler Service"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_alive = True

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_alive = False

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def main(self):
        while self.is_alive:
            # Check if it's time to run the script (e.g., daily at 10:30 AM)
            if time.localtime().tm_hour == 10 and time.localtime().tm_min == 30:
                self.run_script()
            time.sleep(60)  # Wait for 60 seconds before checking again

    def run_script(self):
        subprocess.run(["C:\\LucidMA\\venv\\Scripts\\python.exe", "C:\\LucidMA\\Reporting\\Draft.py"])

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(SchedulerService)