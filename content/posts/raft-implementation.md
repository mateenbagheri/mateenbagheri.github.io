+++
date = '2026-01-07T17:43:00+03:30'
title = 'My experience implementing a Raft consensus algorithm through Hashicorp Raft library'
tags = ['Raft', 'Distributed Systems', 'Hashicorp']
+++

The past few months have been challenging to say the least. During tough times, I find it hard to pour time into my personal projects. However, I started pushing through last month and decided to go back to working on my personal project, [Memorabilia](https://github.com/mateenbagheri/memorabilia). Although it's a bare minimum project at the moment, it is living rent-free on my mind.

One of the paths I decided to go for in this project is to support replication in it using [Raft](https://raft.github.io/). I know there will be a latency tradeoff that I am sacrificing due to this decision, but to me, the project is a playground to learn, so I don't really mind.

This blog post is a collection of small, interesting things I have encountered in my learning of Raft and adding it to Memorabilia.

This is not a technical deep dive and is not meant to be. What the goal for this blog post is, is to set up a baseline on what Raft is, a comparison between two great and well-received libraries written in Go, and what interfaces one needs to implement in order to get a Raft consensus running in one of the two libraries mentioned.

### What is Raft?

Based off of [Raft paper](https://raft.github.io/raft.pdf):

> Raft is a consensus algorithm for managing a replicated log.

*What is a consensus algorithm?* The great challenge in a distributed environment, where there can be multiple instances of our application created, is to reach an agreement on shared decisions (a.k.a.: consensus). Raft is one of the major algorithms that has taken on how to solve this problem originating back in 2013. Even to this date, we see this algorithm, at least some variants of it, [making the headlines](https://www.confluent.io/blog/why-replace-zookeeper-with-kafka-raft-the-log-of-all-logs/) in industry.

Consensus and, in overall distributed systems, is not a modern problem by any means. We have papers, a paper issued back in 1978 by Leslie Lamport named [Time, Clocks, and the Ordering of Events in a Distributed System](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) talking about how ordering of events should be handled in a distributed system. Even before Raft, there were many algorithms having their own take on how to solve consensus in a distributed environment. The most famous and widely used of which is [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)). However, Raft has a rather simpler take on this issue compared to Paxos, making it easier to learn/implement.

### Why adding Raft as a replication protocol shifts my key-value store away from its intended origin and how

As I stated in the prologue, Raft usage in my personal in-memory data store project is a bold choice. As you have seen until now, Raft solves a harder problem than basic in-memory replication. There are tons of simpler protocols that get the job done. By adding this feature to my project, the project stops being just another Redis-like key-value clone, but a *distributed, strongly consistent* key-value store. I would even argue that replication is not the intent. Replication is just what happens when logs are repeated in the same order thanks to consensus.

Like we've seen so far, Raft tackles a much harder problem than what basic in-memory replication really needs. There are tons of simpler protocols that could get the job done. But by adding this feature, the project stops being just another Redis-like key-value clone. Instead, it becomes something else: a distributed, strongly consistent store. And I'd even argue the point isn't really about replication at all. Replication is just the side effect—it's what happens when you force logs to repeat in the same exact order, thanks to consensus. That's the real heart of it.

I'll take Redis project as an example and try to explain how fundamentally different a project becomes due to this choice:

Redis primarily uses asynchronous master-replica replication for data synchronization. There is no consensus. A primary replica is responsible for replicating data.

This method creates some cons:

- If the primary node goes down, there will be data loss.
- There is also no ordering guarantee. The only ordering guarantee you will get is the ordering of the primary node.
- It is eventually consistent. Eventually being the key word.

And some major pros that cannot be ignored for some use cases:

- In a design like Redis's replication, replication should never block the primary, which reduces latency.
- It is simpler to deploy.
- Enables real-time use cases.

There is no good or bad in this comparison between a synchronous and asynchronous approach. Just like everything in our field, there are tradeoffs. There are some projects like [TiKV](https://tikv.org/) going the consensus route to address different requirements for different projects, and I think that's what I am willing to go for with Memorabilia.

### There is no single interface implementation for Raft

At first, I decided to start small and build a really minimal application that uses Raft as its consensus to do three basic operations on a map: Set, Get, and Delete.

When looking around, I came across [many libraries](https://raft.github.io/#implementations), two of which had my attention, one by [etcd](https://github.com/etcd-io/raft) and the other by [Hashicorp](https://github.com/hashicorp/raft). The first thing I noticed was that they differ vastly in the interface implementation they offer.

- Hashicorp offers a way more high-level interface design and lets you focus on designing your state machine. On the other hand, etcd's implementation is way more low-level and thus, it will be harder to implement. This is just a guess, but I believe the reason for this is due to Raft being a core algorithm in etcd that everything is built around it rather than a tool. I am not suggesting that a project like [Vault](https://www.hashicorp.com/en/products/vault) isn't massively benefiting from a consensus algorithm, but I believe the implementation is not built around it, and having a fine-grained control over the algorithm, even if it's a plus, is not a priority.
- Hashicorp's raft implementation comes with a built-in integrated log store named [Raft BoltDB](https://github.com/hashicorp/raft-boltdb). This is while [etcd] has its own custom WAL (which stands for Write Ahead Log). You can read more about it [here](https://developer.hashicorp.com/vault/docs/internals/integrated-storage). Since the log is the main source of truth, their different approach to it is a big deal. Here we see again that Hashicorp decided to opt for simplicity rather than fine-grained control over how things are done.
- How networking is managed in etcd's library is your responsibility. However, in Hashicorp's implementation, this is abstracted (as I will explain further in the implementation section). You will implement a `Transport` interface.

I, as someone who had close to no experience with a distributed environment, chose to start with Hashicorp's library because it was simpler to learn and get my hands on. I could focus on learning the core concepts and worry less about the fine-grained implementations. And that's what I did. Eventually, I might also try to do an etcd implementation. The minimal pure implementation is more preferred for a custom system like mine.

### Implementation

Note that from now onwards, we are only discussing the Hashicorp Raft library. As mentioned before, the implementations and interface design between any two libraries may vary.

I also will not dig too deep into implementation. There are many great implementations that have done this better than me. However, what I struggled while trying to do my implementation was understanding the reasoning behind this interface design and how all of its components contribute to a greater picture.

Here is `NewRaft` function with its documentation:

```go
// NewRaft is used to construct a new Raft node. It takes a configuration, as well
// as implementations of various interfaces that are required. If we have any
// old state, such as snapshots, logs, peers, etc., all those will be restored
// when creating the Raft node.
func NewRaft(
    conf *Config, 
    fsm FSM, 
    logs LogStore, 
    stable StableStore, 
    snaps SnapshotStore, 
    trans Transport,
) (*Raft, error)
```
I know it can feel overwhelming at first. Having to implement 5 interfaces seems daunting. But worry not! Based on your needs, you might not have to implement every single one of the ones mentioned above. For example, as mentioned in the article, you can use `raftboltdb` instance as LogStore and StableStore; You also could use `NewTCPTransport` provided by the package, or for config, you might want to opt for `raft.DefaultConfig()` until you want to tinker around or have specific needs. Regardless, knowing what each of them does won't hurt. If anything, once you understand what problem each interface is responsible for, the design starts to make sense.

In order to make an instance work, you need to implement 4 main interfaces:

### FSM interface 

FSM stands for [Finite State Machine](https://en.wikipedia.org/wiki/Finite-state_machine). I will quote [Hashicorp's article](https://developer.hashicorp.com/nomad/docs/architecture/cluster/consensus) on what this is intended to be:
> An FSM is a collection of finite states with transitions between them. As new logs are applied, the FSM is allowed to transition between states. Application of the same sequence of logs must result in the same state, meaning behavior must be deterministic.

This interface is, basically, the heart of your application logic and domain. You find yourself tinkering a lot with this interface's implementation. Raft does not know or care particularly what your application is doing. It only cares about making sure that each command is ordered, replicated, and committed consistently for every node. Raft ensures that the `Apply()` is called in the same order on each node.

### LogStore interface

Consists of a lot of functions, but eventually, its goal is to dictate how our Raft log is stored. Machine state can be rebuilt from repeating raft logs in order. For most use cases, the `raftboltdb` provided by Hashicorp will do the job. Unless someone specifically needs a WAL implementation for a specific need, there is no need to implement this interface. Hashicorp itself uses the `raftboltdb` for its own projects, which shows it is stable and reliable.

### StableStore interface

`StableStore`, unlike `LogStore`, dictates how Raft's metadata is going to be stored. This has nothing to do with your application data. Information regarding leader election is included. This data is crucial to the application and its safety in case of unexpected system behaviour is needed. Hashicorp keeps this interface minimal and also provides us with a boltdb backend implementation to use.

### Transport interface 

How two nodes communicate with each other is dictated by this interface. This is where networking comes into play. I will avoid further diving into this interface since all I want to say is that [this article](https://deepwiki.com/hashicorp/raft/4.1-transport-interfaces) does better.

### SnapshotStore interface

This interface does exactly what its name suggests it does. Snapshots, like Finite State Machines, are not terms and concepts reserved for the Raft algorithm. Based on the wiki
> In computer systems, a snapshot is the state of a system at a particular point in time.

Why do we need snapshots in this algorithm? Not having a snapshot handling method will work at first, but consider what happens in a long-running application where logs are getting appended rapidly. I myself thought at first that snapshots must be optional. But when you think about it, most modern applications will get hurt by not implementing it in the long term. Snapshotting is a simple way to handle an ever-growing log backlog. I will quote section 7 in the raft paper:
> Raft’s log grows during normal operation to incorporate more client requests, but in a practical system, it cannot grow without bound. As the log grows longer, it occupies more space and takes more time to replay. This will eventually cause availability problems without some mechanism to discard obsolete information that has accumulated in the log. Snapshotting is the simplest approach to compaction. In snapshotting, the entire current system state is written to a snapshot on stable storage, then the entire log up to that point is discarded.

### The greater picture

When I get back to when I decided to start my small implementation. I remember I saw each of these interfaces as a task to be implemented. However, when I finished my implementation, I noticed how little I knew in the beggining. They are not separate as much as I thought they were; quite the contrary they were all building up and supporting other interfaces to reach the same eventual goal: **distributed consensus**

`LogStore` ensures every node sees the same order of commands. `FSM` applies those commands and moves the system from one state to next. `StableStore` holds metadata that keeps election safe and recoverable. `SnapshotStore` keeps the system reliable for longer time by preventing all logs growing forever, and finally, `Transport` binds two nodes together allowing them to communicate to each other.

At the end of the day, Raft isn’t just an algorithm you add to a project. It’s a mindset. It forces you to think about what consistency really means, how systems fail, and what it takes to keep them running. Even if you are trying to learn something trivial like building a key-value store or a complext service or just trying it out, working with Raft changes how you see distributed systems not as a collection of independent nodes, but as a single whole that survives with being dependant in a ironic way.

***
Thank you for reading. If you’re curious about the implementation or want to follow along as Memorabilia evolves, you can find the project on my GitHub. Feel free to reach out with questions and corrections or just to have a chat on distributed systems. I am not claiming to be an expert in this topic so I may have a few things missed or misrepresent here. Have a great day [ or night depending on where on this orb you live]!
