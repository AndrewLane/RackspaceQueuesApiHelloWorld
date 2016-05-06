using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using net.openstack.Providers.Rackspace;
using net.openstack.Core.Providers;
using net.openstack.Core.Exceptions.Response;
using net.openstack.Providers.Rackspace.Objects;
using net.openstack.Core.Domain;
using net.openstack.Core.Domain.Queues;
using net.openstack.Core.Collections;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Configuration;

namespace RackspaceQueuesApiHelloWorld
{
    class Program
    {
        static string apiKey = "";
        static string userName = "";
        static string region = "";
        static string queueName = "";
        const int millisecondsDelayForProcessing = 3000;

        static CloudQueuesProvider cloudQueuesProvider = null;

        static void Main()
        {
            apiKey = ConfigurationManager.AppSettings["RackspaceApiKey"];
            userName = ConfigurationManager.AppSettings["RackspaceUsername"];
            region = ConfigurationManager.AppSettings["RackspaceRegion"];
            queueName = ConfigurationManager.AppSettings["RackspaceQueueName"];

            try
            {
                cloudQueuesProvider =
                      new CloudQueuesProvider(
                              defaultIdentity: new CloudIdentity()
                              {
                                  APIKey = apiKey,
                                  Username = userName
                              },
                              defaultRegion: region,
                              clientId: Guid.NewGuid(),
                              internalUrl: false,
                              identityProvider: null);

                FetchQueues();
                MainLoop();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Fail! {0}", ex);
            }

            Console.WriteLine("Exiting application");
        }

        /// <summary>
        /// Fetches all the queues and lists them out
        /// </summary>
        static void FetchQueues()
        {
            Console.WriteLine("Looking up queues...");
            var queueList = cloudQueuesProvider.ListQueuesAsync(marker: null, limit: null, detailed: true, cancellationToken: CancellationToken.None).Result;

            Console.WriteLine(String.Format("You have {0} queues.", queueList.Count));
            foreach (var queue in queueList)
            {
                Console.WriteLine("{0} {1}", queue.Name, queue.Href);
            }
        }

        /// <summary>
        /// Interactive loop where the user can push, pop, and peek at messages
        /// </summary>
        static void MainLoop()
        {
            string command = String.Empty;
            bool done = false;
            while (done == false)
            {
                Console.WriteLine("Enter peek, pop, push, or done:");
                command = Console.ReadLine().ToLower();
                if (command == "push")
                {
                    var queueToAddMessageTo = new QueueName(queueName);
                    var ttl = TimeSpan.FromMinutes(5);
                    JObject message_body = JObject.Parse(String.Format("{{\"foo\": \"{0}\"}}", DateTime.Now.ToString("u")));
                    Message message = new Message(ttl, message_body);
                    Message[] messages = { message };
                    MessagesEnqueued queuedMsg = cloudQueuesProvider.PostMessagesAsync(queueToAddMessageTo, CancellationToken.None, messages).Result;
                    foreach (var id in queuedMsg.Ids)
                    {
                        Console.WriteLine("Queued message {0}", id.Value);
                    }
                }
                else if (command == "peek")
                {
                    var queueToPeek = new QueueName(queueName);
                    var ttl = TimeSpan.FromMinutes(5);
                    var grace = TimeSpan.FromMinutes(5);
                    Claim claim = cloudQueuesProvider.ClaimMessageAsync(
                          queueToPeek,
                          limit: 1,
                          timeToLive: ttl,
                          gracePeriod: grace,
                          cancellationToken: CancellationToken.None).Result;
                    if (claim.Messages.Any() == false)
                    {
                        Console.WriteLine("No messages to peek");
                        continue;
                    }
                    Console.WriteLine("Peeking {0}...", claim.Messages.First().Body);
                    Task.Delay(millisecondsDelay: 3000).Wait();
                    cloudQueuesProvider.ReleaseClaimAsync(queueToPeek, claim, CancellationToken.None).Wait();

                    Console.WriteLine("Released claim on message");
                }
                else if (command == "pop")
                {
                    var queueToPop = new QueueName(queueName);
                    var ttl = TimeSpan.FromMinutes(5);
                    var grace = TimeSpan.FromMinutes(5);
                    Claim claim = cloudQueuesProvider.ClaimMessageAsync(
                          queueToPop,
                          limit: 1,
                          timeToLive: ttl,
                          gracePeriod: grace,
                          cancellationToken: CancellationToken.None).Result;
                    if (claim.Messages.Any() == false)
                    {
                        Console.WriteLine("No messages to pop");
                        continue;
                    }
                    var msgToProcess = claim.Messages.First();
                    Console.WriteLine("Processing {0}...", msgToProcess.Body);
                    Task.Delay(millisecondsDelay: millisecondsDelayForProcessing).Wait();

                    cloudQueuesProvider.DeleteMessageAsync(queueToPop, msgToProcess.Id, claim, CancellationToken.None).Wait();

                    Console.WriteLine("Message deleted from queue");
                }
                else if (command == "done")
                {
                    done = true;
                }
            }
        }
    }

}
