import numpy as np
import seaborn as sns

from SimulateQueue import simulate_queue

np.random.seed(15)


if __name__ == '__main__':
    lambdas = np.random.uniform(2, 20, 4)
    print('lambdas:', lambdas)
    mu = sum(lambdas) + 5
    print('mu:', mu)

    # run simulation
    df = simulate_queue(arrival_rates=lambdas, service_rate=mu)
    df['job'] = df.index
    df['priority'] = df['priority'].replace({0: 'high', 1: 'medium', 2: 'normal', 3: 'low'})
    df['time_in_system'] = df['exit_time'] - df['enter_time']
    df.to_csv('simulations/queue.csv', index=False)

    # system time plot
    sns.set(rc={'figure.figsize': (10, 5)})
    plot = sns.lineplot(df, x='job', y='time_in_system', hue='priority', hue_order=['low', 'normal', 'medium', 'high'],
                        palette='magma')
    plot.set(xlabel='Job', ylabel='Time in System', title='Network Traffic')
    plot.get_figure().savefig('plots/sys_plot.png', dpi=400)
    plot.get_figure().clf()

    # latency
    latency = df.groupby('priority').agg(['min', 'max', 'mean'])['time_in_system']
    latency.to_csv('simulations/latency.csv')
    normalized_latency = latency.apply(lambda x: (x - x.mean()) / x.std())
    normalized_latency.reset_index(inplace=True)
    normalized_latency = normalized_latency.melt(id_vars='priority')

    sns.set()
    plot = sns.barplot(normalized_latency, x='priority', y='value', hue='variable', palette='flare',
                       order=['low', 'normal', 'medium', 'high'])
    plot.set(xlabel='Priority', ylabel='Latency', title='Network Latency')
    plot.get_figure().savefig('plots/latency.png', dpi=400)
    plot.get_figure().clf()

    # throughput
    throughput = np.ceil(df['exit_time']).value_counts()
    throughput = throughput.describe().to_frame('throughput').loc[['min', 'max', 'mean']]
    throughput['temp'] = None
    throughput = throughput.reset_index().melt(id_vars='index').dropna()

    plot = sns.barplot(throughput, x='variable', y='value', hue='index', palette='flare')
    plot.set(xlabel='', title='Network Throughput')
    plot.get_figure().savefig('plots/throughput.png', dpi=400)
    plot.get_figure().clf()

    # jitter
    jitter = df.groupby('priority').std()['time_in_system']
    jitter.loc['overall'] = df['time_in_system'].std()
    jitter.to_csv('simulations/jitter.csv')

    plot = sns.kdeplot(df, x='time_in_system', hue='priority', palette='flare', multiple="stack",
                       hue_order=['low', 'normal', 'medium', 'high'])
    sns.kdeplot(df, x='time_in_system')
    plot.set(xlabel='Time in System', title='Network Jitter')
    plot.get_figure().savefig('plots/jitter.png', dpi=400)
    plot.get_figure().clf()
