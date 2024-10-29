module Arg = Stdlib.Arg

open Httpun_unix
open Httpun

let error_handler (_ : Unix.sockaddr) = Httpun_examples.Server.error_handler

let request_handler (_ : Unix.sockaddr) { Gluten.reqd; _ } =
    match Reqd.request reqd  with
    | { Request.meth = `POST; headers; _ } ->
      let response =
        let content_type =
          match Headers.get headers "content-type" with
          | None   -> "application/octet-stream"
          | Some x -> x
        in
        Response.create
          ~headers:(Headers.of_list
            [ "content-type", content_type;
            "transfer-encoding", "chunked"
            (* ; "connection", "close" *)
            ]) `OK
      in
      let request_body  = Reqd.request_body reqd in
      let response_body = Reqd.respond_with_streaming reqd response in
      let rec on_read buffer ~off ~len =
        Body.Writer.write_bigstring response_body buffer ~off ~len;
        Body.Reader.schedule_read request_body ~on_eof ~on_read;
      and on_eof () =
        Stdlib.Format.eprintf "EOF@.";
        Body.Writer.close response_body
      in
      Body.Reader.schedule_read (Reqd.request_body reqd) ~on_eof ~on_read
    | _ ->
      let headers = Headers.of_list [ "connection", "close" ] in
      Reqd.respond_with_string reqd (Response.create ~headers `Method_not_allowed) ""


let _log_connection_error ex =
  Printf.printf "Uncaught exception handling client: %s" (Printexc.to_string ex)

let connection_handler client_addr socket =
  Server.create_connection_handler ~request_handler ~error_handler client_addr socket


let main port =
  let sockaddr = Unix.ADDR_INET (Unix.inet_addr_any, port) in
  let domain = Unix.domain_of_sockaddr sockaddr in
  let sock = Unix.socket domain Unix.SOCK_STREAM 0 in
  Unix.setsockopt sock Unix.SO_REUSEADDR true ;
  Unix.bind sock sockaddr ;
  Unix.listen sock 5 ;
  Printf.printf "  echo \"Testing echo POST\" | curl -XPOST --data @- http://localhost:%d\n\n%!" port;
  while true do
    let s, caller = Unix.accept ~cloexec:true sock in
    connection_handler caller s
  done

let () =
  let port = ref 8080 in
  Arg.parse
    ["-p", Arg.Set_int port, " Listening port number (8080 by default)"]
    ignore
    "Echoes POST requests. Runs forever.";
  main !port
;;
