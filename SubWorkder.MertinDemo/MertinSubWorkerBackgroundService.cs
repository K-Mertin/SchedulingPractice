using Microsoft.Extensions.Hosting;
using SchedulingPractice.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace SubWorker.MertinDemo
{
    public class MertinSubWorkerBackgroundServices : BackgroundService
    {
        private BlockingCollection<JobInfo> _jobsCollection = new BlockingCollection<JobInfo>();

        private int _numOfTask = 5;
        private string GetNow()
        {
            return DateTime.Now.ToString("hh:mm:ss.fff");
        }
        private void Worker(CancellationToken stoppingToken)
        {
            var tid = Thread.CurrentThread.ManagedThreadId;
            Console.WriteLine($"{GetNow()}-Worker[{tid}]-Start");

            using (JobsRepo repo = new JobsRepo())
            {           
                foreach (var job in _jobsCollection.GetConsumingEnumerable())
                {
                    //Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-Get");

                    TimeSpan randomDelay = job.RunAt - DateTime.Now - TimeSpan.FromMilliseconds(new Random().Next(500, 1000));

                    if (randomDelay > TimeSpan.Zero)
                    {
                        //Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-Sleep");
                        if (stoppingToken.WaitHandle.WaitOne(randomDelay))
                        {
                            Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-Cancel");
                            break;
                        }
                    }

                    if (repo.GetJob(job.Id).State != 0)
                        continue;                    

                    if (repo.AcquireJobLock(job.Id))
                    {
                        Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-Lock");

                        TimeSpan waitTime = job.RunAt - DateTime.Now;

                        if (waitTime > TimeSpan.Zero)
                        {
                            Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-Sleep After Lock-{waitTime}");
                            try
                            {
                                Thread.Sleep(waitTime);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-Sleep Exception {ex.Message.ToString()}");

                            }
                        }
                        Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-Before Process");
                        repo.ProcessLockedJob(job.Id);
                        //Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-{GetNow()}-Completed");
                        Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-{(DateTime.Now - job.RunAt)}-Delay");
                        //Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-{job.RunAt.ToString("hh:mm:ss.fff")}-Should Run At");
                        
                    }
                    else
                    {
                        Console.WriteLine($"{GetNow()}-Worker[{tid}]-Job{job.Id}-Collision");
                    }

                }
                //Console.WriteLine($"{GetNow()}-Worker[{tid}]-Receive Cancel Message");
            }
            Console.WriteLine($"{GetNow()}-Worker[{tid}]-Exit");
        }

     
        
        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var pid = Process.GetCurrentProcess().Id;
            var tid = Thread.CurrentThread.ManagedThreadId;

            Console.WriteLine($"{GetNow()}-Producer[{pid}-{tid}]");
            await Task.Delay(1);

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < _numOfTask; i++)
            {
                Task t = Task.Run(() => Worker(stoppingToken));
                tasks.Add(t);
            }

            using (JobsRepo repo = new JobsRepo())
            {
                while (stoppingToken.IsCancellationRequested == false)
                {
                    Console.WriteLine($"{GetNow()}-Producer[{pid}-{tid}]-Get Jobs");
                    
                    var readyJobs = repo.GetReadyJobs(JobSettings.MinPrepareTime);               

                    foreach (var job in readyJobs)
                    {
                        if (stoppingToken.IsCancellationRequested)
                            break;

                        _jobsCollection.Add(job);
                        //Console.WriteLine($"{GetNow()}-Producer[{pid}-{tid}]- Put Job{job.Id}");
                    }
                    //Console.WriteLine($"{GetNow()}-Producer[{pid}-{tid}]-Sleep");

                    if (stoppingToken.WaitHandle.WaitOne(JobSettings.MinPrepareTime-TimeSpan.FromSeconds(1)))
                    { 
                        Console.WriteLine($"{GetNow()}-Producer[{pid}-{tid}]-Inner Cancel");
                        break;
                    }
                }
                Console.WriteLine($"{GetNow()}-Producer[{pid}-{tid}]-Outter Cancel");
                _jobsCollection.CompleteAdding();

            }
            await Task.WhenAll(tasks);            
            Console.WriteLine($"{GetNow()}-Producer[{pid}]-Exit");
        }
    }

}
