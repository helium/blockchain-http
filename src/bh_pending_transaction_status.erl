-module(bh_pending_transaction_status).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3]).

init(Choices, _) -> Choices.

names() ->
	[pending_transaction_status].

encode(Atom, pending_transaction_status, Choices) when is_atom(Atom) ->
	true = lists:member(Atom, Choices),
	atom_to_binary(Atom, utf8);
encode(Binary, pending_transaction_status, Choices) ->
	true = lists:member(binary_to_existing_atom(Binary, utf8), Choices),
	Binary.

decode(Bin, pending_transaction_status, Choices) ->
	Atom = binary_to_existing_atom(Bin, utf8),
	true = lists:member(Atom, Choices),
	Atom.
