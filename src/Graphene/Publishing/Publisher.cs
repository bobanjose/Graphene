// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using Graphene.Tracking;

namespace Graphene.Publishing
{
    internal class Publisher
    {
        private static ActionBlock<ContainerBase> _trackerBlock;
        private static ActionBlock<Data.TrackerData> _publisherBlock;
        
        private static ContainerBase _firstTC;
        private static System.Threading.CancellationTokenSource _trackerBlockCancellationTokenSource = new System.Threading.CancellationTokenSource();

        private static bool _lastPersistanceComplete;

        static Publisher()
        {
            _trackerBlock = new ActionBlock<ContainerBase>(async tc =>
            {
                try
                {
                    var trackerContainer = (ContainerBase)tc;
                    if (_firstTC == null)
                        _firstTC = trackerContainer;
                    else if (_firstTC == trackerContainer && !_trackerBlockCancellationTokenSource.IsCancellationRequested)
                    {
                        try
                        {
                            await Task.Delay(TimeSpan.FromSeconds(30), _trackerBlockCancellationTokenSource.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            System.Diagnostics.Debug.WriteLine("Task.Delay exiting");
                        }
                    }

                    foreach (var td in trackerContainer.GetTrackerData(_trackerBlockCancellationTokenSource.IsCancellationRequested))
                    {
                        _publisherBlock.Post(td);
                    }
                }
                catch (Exception ex)
                {
                    Configurator.Configuration.Logger.Error(ex.Message, ex);
                }
                finally
                {
                    if (!_trackerBlockCancellationTokenSource.IsCancellationRequested)
                        _trackerBlock.Post(tc);
                    else if (_trackerBlock.InputCount == 0)
                        _trackerBlock.Complete();
                }
                
            });

            _publisherBlock = new ActionBlock<Data.TrackerData>(tc =>
            {
                try
                {
                    _lastPersistanceComplete = false;
                    Configurator.Configuration.Persister.Persist(tc);
                    _lastPersistanceComplete = true;
                }
                catch (Exception ex)
                {
                    Configurator.Configuration.Logger.Error(ex.Message, ex);
                    _lastPersistanceComplete = true;
                }                
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 4});
        }        

        internal static void Register(ContainerBase trackerContainer)
        {
            _trackerBlock.Post(trackerContainer);
        }

        internal static void ShutDown()
        {
            _trackerBlockCancellationTokenSource.Cancel();
            Task.WaitAll(_trackerBlock.Completion);
            var loopCount = 0;
            while ((_publisherBlock.InputCount > 0 || !_lastPersistanceComplete)  && loopCount < 20)
            {
                System.Threading.Thread.Sleep(100);
                loopCount++;
            }
            if(_publisherBlock.InputCount > 0)
                Configurator.Configuration.Logger.Error(_publisherBlock.InputCount + " messages could not be persisted.", new Exception("Graphene couldn't persist all message to persister."));
        }
    }
}
