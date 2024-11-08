module Buffer = Gluten.Buffer

module IO_loop = struct
  let writev socket iovecs =
    try
      let lenv =
        List.fold_left
          (fun acc { Faraday.buffer; off; len } ->
            let _ : int = 
              let b = Bigarray.Array1.sub buffer off len in
              Printf.printf "%d: writev: %s\n%!" Thread.(self () |> id) (Bigstringaf.to_string b) ;
              Bigstring_unix.write socket ~len b
            in
            acc + len
           )
          0
          iovecs
      in
      `Ok lenv
    with End_of_file -> `Closed

  let read_once socket buffer =
    Buffer.put
      ~f:(fun buf ~off ~len k ->
        let n =
          (* OCaml 5.2 has Unix.read_bigarray, but use Bigstring_unix for now *)
          Bigarray.Array1.sub buf off len
          |> Bigstring_unix.read socket ~len
        in
        Printf.printf "%d: read %d\n%!" Thread.(self () |> id) n ;
        match n with
        | 0 -> raise End_of_file
        | _ -> k n
      )
      buffer
      (fun _ -> ())

  let read socket buffer =
    match read_once socket buffer with
    | r -> r
    | exception (
      Unix.Unix_error (ENOTCONN, _, _) 
      | Unix.Unix_error (EBADF, _, _) ) ->
        raise End_of_file

  let shutdown socket cmd =
    try Unix.shutdown socket cmd with
    | Unix.Unix_error (ENOTCONN, _, _) 
    | Unix.Unix_error (EBADF, _, _) ->
      ()

  let close socket =
    try Unix.close socket with
    | Unix.Unix_error (ENOTCONN, _, _) 
    | Unix.Unix_error (EBADF, _, _) ->
      ()

  let start : type t.
    (module Gluten.RUNTIME with type t = t)
    -> read_buffer_size:int
    -> t
    -> Unix.file_descr
    -> unit
    =
   fun (module Runtime) ~read_buffer_size t socket ->
    Printf.printf "%d: IO_loop.start\n%!"  Thread.(self () |> id);
    let write_closed = ref false in
    let read_buffer = Buffer.create read_buffer_size in
    let rec read_loop () =
        Printf.printf "%d: read_loop\n%!"  Thread.(self () |> id);
        let rec read_loop_step () =
          Printf.printf "%d: read_loop_step\n%!"  Thread.(self () |> id);
          match Runtime.next_read_operation t with
          | `Read ->
            Printf.printf "%d: read_loop_step: read\n%!"  Thread.(self () |> id);
            (match read socket read_buffer with
            | _n ->
              let (_ : int) =
                Buffer.get read_buffer ~f:(fun buf ~off ~len ->
                  Runtime.read t buf ~off ~len)
              in
              ()
            | exception End_of_file ->
              let (_ : int) =
                Buffer.get read_buffer ~f:(fun buf ~off ~len ->
                  Runtime.read_eof t buf ~off ~len)
              in
              ());
            read_loop_step ()
          | `Yield ->
            Printf.printf "%d: read_loop_step: yield\n%!"  Thread.(self () |> id);
            Runtime.yield_reader t read_loop
          | `Close ->
            Printf.printf "%d: read_loop_step: close\n%!"  Thread.(self () |> id);
            (match read socket read_buffer with
            | _n ->
              (* discard *)
              ()
            | exception (End_of_file as exn) ->
              shutdown socket Unix.SHUTDOWN_RECEIVE ;
              (match !write_closed with
              | true ->
                (* If the write loop has finished, the loop is closing
                   cleanly. We don't need to do anything else. *)
                ()
              | false ->
                (* If the write loop hasn't yet finished, but we got EOF from
                   read (i.e. socket closed), we want to feed EOF to the
                   writer here so that we can terminate cleanly. *)
                Runtime.report_exn t exn)
              )
        in
        match read_loop_step () with
        | () -> ()
        | exception exn -> print_endline (Printexc.to_string exn) ; Runtime.report_exn t exn
    in
    let rec write_loop () =
      Printf.printf "%d: write_loop\n%!"  Thread.(self () |> id);
      let rec write_loop_step () =
        Printf.printf "%d: write_loop_step\n%!"  Thread.(self () |> id);
        match Runtime.next_write_operation t with
        | `Write io_vectors ->
          Printf.printf "%d: write_loop_step: write\n%!"  Thread.(self () |> id);
          let write_result = writev socket io_vectors in
          Runtime.report_write_result t write_result;
          write_loop_step ()
        | `Yield ->
          Printf.printf "%d: write_loop_step: yield\n%!"  Thread.(self () |> id);
          Runtime.yield_writer t write_loop
        | `Close _ ->
          Printf.printf "%d: write_loop_step: close\n%!"  Thread.(self () |> id);
          write_closed := true;
          shutdown socket Unix.SHUTDOWN_SEND
      in
      match write_loop_step () with
      | () -> ()
      | exception exn -> print_endline (Printexc.to_string exn) ; Runtime.report_exn t exn
    in
    let _ : Thread.t = Thread.create (fun () -> 
      Runtime.yield_writer t write_loop ;
      read_loop ();
      (*
      let read_thread = Thread.create (fun () -> read_loop (); Printf.printf "%d: ! read loop ended\n%!" Thread.(self () |> id)) () in
      write_loop ();
      Printf.printf "%d: ! write loop ended\n%!" Thread.(self () |> id);
      Thread.join read_thread ;*)
      Printf.printf "%d: ! closing socket\n%!" Thread.(self () |> id);
      close socket
    ) () in
    ()
end

module Server = struct
  let create_connection_handler
        ~read_buffer_size
        ~protocol
        connection
        _client_addr
        socket
    =
    let connection = Gluten.Server.create ~protocol connection in
    IO_loop.start
      (module Gluten.Server)
      ~read_buffer_size
      connection
      socket

  let create_upgradable_connection_handler
        ~read_buffer_size
        ~protocol
        ~create_protocol
        ~request_handler
        (client_addr : Unix.sockaddr)
        socket
    =
    let connection =
      Gluten.Server.create_upgradable
        ~protocol
        ~create:create_protocol
        (request_handler client_addr)
    in
    Printf.printf "%d: Gluten_unix.Server.create_upgradable_connection_handler\n%!" Thread.(self () |> id);
    IO_loop.start
      (module Gluten.Server)
      ~read_buffer_size
      connection
      socket
end
