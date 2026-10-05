package com.example.ai.domain.session.model.valobj;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SessionConfigV0 {
    private String sessionId;

    /*
    * 这是 Spring WebFlux / Reactor 中用于处理 SSE（Server-Sent Events，服务器推送事件）的一个核心对象。
    * Sinks.Many<...>	一个可以手动塞数据、支持多订阅者的“事件池”
    * SSE ： HTTP 协议下的一种“服务器持续向浏览器推送消息”的技术。
    * */
    private Sinks.Many<ServerSentEvent<String>> sink;

    private Instant createTime;

    /*
    * volatile 是 Java 中用于保证多线程之间变量可见性的关键字。
    * 作用：
    * 可见性：一个线程修改了这个变量的值，其他线程立刻就能看到最新值（而不是从自己的 CPU 缓存里读旧值）。
    * 禁止指令重排序：防止编译器和 CPU 为了优化而打乱代码执行顺序，导致并发问题。
    * 不保证原子性
    * */
    private volatile Instant lastAccessedTime;

    private volatile boolean active;

    public SessionConfigV0(String sessionId, Sinks.Many<ServerSentEvent<String>> sink) {
        this.sessionId = sessionId;
        this.sink = sink;
        this.createTime = Instant.now();
        this.lastAccessedTime = Instant.now();
        this.active = true;
    }

    public void markInactive(){
        this.active = false;
    }

    public void updateLastAccessed(){
        this.lastAccessedTime = Instant.now();
    }

    public boolean isExpired(long timeoutMinutes){
        return lastAccessedTime.isBefore(Instant.now().minus(timeoutMinutes, ChronoUnit.MINUTES));
    }
}
