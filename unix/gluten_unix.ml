module Buffer = Gluten.Buffer

module IO_loop = struct
  let writev socket iovecs =
    try
      let lenv =
        List.fold_left
          (fun acc { Faraday.buffer; off; len } ->
            let _ : int = 
              Bigarray.Array1.sub buffer off len
              |> Bigstring_unix.write socket ~len
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
        (* OCaml 5.2 has Unix.read_bigarray, but use Bigstring_unix for now *)
        Bigarray.Array1.sub buf off len
        |> Bigstring_unix.read socket ~len
        |> k
      )
      buffer
      (fun _ -> ())

  let read socket buffer =
    match read_once socket buffer with
    | r -> r
    | exception
        ( Unix.Unix_error (ENOTCONN, _, _)) ->
      raise End_of_file

  let shutdown _socket _cmd =
    ()
    (* 
    try Eio.Flow.shutdown flow cmd with
    | Unix.Unix_error (ENOTCONN, _, _) ->
      () *)

  let start : type t.
    (module Gluten.RUNTIME with type t = t)
    -> read_buffer_size:int
    -> t
    -> Unix.file_descr
    -> unit
    =
   fun (module Runtime) ~read_buffer_size t socket ->
    let write_closed = ref false in
    let read_buffer = Buffer.create read_buffer_size in
    let rec read_loop =
      fun () ->
        let rec read_loop_step () =
          match Runtime.next_read_operation t with
          | `Read ->
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
            Runtime.yield_reader t (Fun.id);
            read_loop ()
          | `Close ->
            ()
        in
        match read_loop_step () with
        | () -> ()
        | exception exn -> Runtime.report_exn t exn
    in
    let rec write_loop () =
      let rec write_loop_step () =
        match Runtime.next_write_operation t with
        | `Write io_vectors ->
          let write_result = writev socket io_vectors in
          Runtime.report_write_result t write_result;
          write_loop_step ()
        | `Yield ->
          Runtime.yield_writer t (Fun.id);
          write_loop ()
        | `Close _ ->
          write_closed := true;
          shutdown socket `Send
      in
      match write_loop_step () with
      | () -> ()
      | exception exn -> Runtime.report_exn t exn
    in
    let _ : Thread.t = Thread.create (fun () -> read_loop) () in
    let _ : Thread.t = Thread.create (fun () -> write_loop) () in
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
    IO_loop.start
      (module Gluten.Server)
      ~read_buffer_size
      connection
      socket
end
