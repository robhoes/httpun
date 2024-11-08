module Arg = Stdlib.Arg

open Httpun_unix
open Httpun

let error_handler (_ : Unix.sockaddr) = Httpun_examples.Server.error_handler

let request_handler (_ : Unix.sockaddr) { Gluten.reqd; _ } =
    match Reqd.request reqd  with
    | { Request.meth = `POST; headers; _ } ->
      Printf.printf "%d: received request\n%!" Thread.(self () |> id);
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
        Printf.printf "%d: on_read: %s\n%!" Thread.(self () |> id) (Bigstringaf.to_string buffer);
        Body.Writer.write_bigstring response_body buffer ~off ~len;
        Printf.printf "%d: on_read: schedule new read\n%!" Thread.(self () |> id);
        Body.Reader.schedule_read request_body ~on_eof ~on_read;
      and on_eof () =
        Printf.printf "%d: EOF\n%!" Thread.(self () |> id);
        Body.Writer.close response_body
      in
      Printf.printf "%d: reading body\n%!" Thread.(self () |> id);
      Body.Reader.schedule_read request_body ~on_eof ~on_read
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
  Printf.printf "%d  echo \"Testing echo POST\" | curl -XPOST --data @- http://localhost:%d\n\n%!" Thread.(self () |> id) port;
  while true do
    let s, caller = Unix.accept ~cloexec:true sock in
    Printf.printf "%d: Accepted connection\n%!" Thread.(self () |> id);
    connection_handler caller s ;
    Printf.printf "%d: Dispatched connection handler\n%!" Thread.(self () |> id)
  done

let () =
  let port = ref 8080 in
  Arg.parse
    ["-p", Arg.Set_int port, " Listening port number (8080 by default)"]
    ignore
    "Echoes POST requests. Runs forever.";
  main !port
;;
