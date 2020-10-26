xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $session as document-node()? external;
declare variable $endpointState as document-node()? external;
declare variable $workUnit as document-node()? external;
declare variable $input as document-node()* external;

let $primary-name := $workUnit/name/fn:string()
let $keys := $workUnit/keys/fn:string()
let $key-delimiter := $workUnit/keyDelimiter/fn:string()
let $uri-prefix := $workUnit/uriPrefix/fn:string()
let $uri-suffix := $workUnit/uriSuffix/fn:string()
let $collections := $workUnit/collections/fn:string()
let $jsonDoc := xdmp:from-json($doc)
let $_ :=
  for $doc in $input
  let $primary-id :=
    fn:string-join(
      for $key in $keys
        let $s := xdmp:value("$doc/" || $key)
        let $out := if (fn:empty(map:get($jsonDoc,$key))) then "null" else map:get($jsonDoc,$key)
      return $out
      ,
      $key-delimiter
    )
  let $uri := fn:concat($uri-prefix, $primary-id, $uri-suffix)
  return
    xdmp:document-insert($uri, $doc,
      map:map() => map:with("collections", ($collections))
    )

return ()
