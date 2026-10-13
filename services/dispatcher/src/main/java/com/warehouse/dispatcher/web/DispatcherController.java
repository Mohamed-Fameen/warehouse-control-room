package com.warehouse.dispatcher.web;

import com.warehouse.dispatcher.DispatchLoop;
import com.warehouse.dispatcher.board.JobBoard;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DispatcherController {

    private final DispatchLoop loop;

    public DispatcherController(DispatchLoop loop) {
        this.loop = loop;
    }

    /** The jobs this replica holds and what it knows about each robot. */
    @GetMapping("/api/dispatcher")
    JobBoard.Snapshot state() throws Exception {
        return loop.snapshot();
    }
}
