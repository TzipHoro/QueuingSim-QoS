import heapq
import simpy
import numpy as np
import pandas as pd
import seaborn as sns


def simulate_queue(arrival_rates: np.ndarray, service_rate: float, n_sims: int = 5000) -> pd.DataFrame:
    total_jobs = {}

    class PriorityQueue:
        def __init__(self, env: simpy.Environment):
            self.env = env
            self.queue = []
            heapq.heapify(self.queue)
            self.busy = False

        def add(self, job, priority):
            heapq.heappush(self.queue, (priority, job))

        def start(self, server):
            while True:
                if not self.queue:
                    self.busy = False
                    yield self.env.timeout(1)
                else:
                    self.busy = True
                    priority, job = heapq.heappop(self.queue)
                    self.env.process(server.serve(job))

    class Server:
        def __init__(self, env: simpy.Environment, svc_rate):
            self.env = env
            self.mean_service_time = svc_rate

        def serve(self, job):
            service_time = np.random.exponential(scale=self.mean_service_time)
            yield self.env.timeout(service_time)
            job.exit_time = self.env.now
            total_jobs[job.id] = {**total_jobs[job.id], 'exit_time': job.exit_time}
            print(f"Job {job.__str__()} finished at time {job.exit_time}, priority {job.priority}")

    class Job:
        def __init__(self, env: simpy.Environment, job_id: int, priority: int):
            self.env = env
            self.id = job_id
            self.priority = priority
            self.enter_time = None
            self.exit_time = None

        def __str__(self):
            return f"{self.id}-{self.priority}"

        def __lt__(self, other):
            return self.priority < other.priority

        def start(self):
            self.enter_time = self.env.now
            total_jobs[self.id] = {'enter_time': self.enter_time, 'priority': self.priority}
            print(f"Job {self.__str__()} entered the system at time {self.enter_time}, priority {self.priority}")

    class SimQueue:
        def __init__(self, arr_rates, svc_rate):
            self.env = simpy.Environment()
            self.arrival_rates = arr_rates
            self.server = Server(self.env, svc_rate)
            self.queue = PriorityQueue(self.env)
            self.job_id = 0

            for i in range(4):
                self.env.process(self.generate_jobs(i))
                self.env.process(self.process_jobs())

        def start(self, sim_time):
            self.env.run(until=sim_time)

        def generate_jobs(self, class_id):
            while True:
                interarrival_time = np.random.poisson(self.arrival_rates[class_id])
                yield self.env.timeout(interarrival_time)
                self.job_id += 1
                job = Job(self.env, self.job_id, class_id)
                self.queue.add(job, class_id)
                job.start()

        def process_jobs(self):
            while True:
                if not self.queue.busy and self.queue.queue:
                    self.env.process(self.queue.start(self.server))
                yield self.env.timeout(1)

    env = SimQueue(arr_rates=arrival_rates, svc_rate=service_rate)
    env.start(n_sims)

    return pd.DataFrame(total_jobs).T


if __name__ == '__main__':
    np.random.seed(60)
    lambdas = np.random.uniform(2, 60, 4)
    mu = sum(lambdas) + 5

    df = simulate_queue(arrival_rates=lambdas, service_rate=mu)
    df['job'] = df.index
    df['time_in_system'] = df['exit_time'] - df['enter_time']
    df.to_csv('queue.csv')

    # system time plot
    sns.set(rc={'figure.figsize': (9, 5)})
    plot = sns.lineplot(df, x='job', y='time_in_system', hue='priority', palette='flare')
    plot.get_figure().savefig('sys_plot.png')
    plot.get_figure().clf()
