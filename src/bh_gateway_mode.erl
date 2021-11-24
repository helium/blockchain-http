-module(bh_gateway_mode).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3]).


init(Choices, _) -> Choices.

names() ->
	[gateway_mode].

encode(Atom, gateway_mode, Choices) when is_atom(Atom) ->
	lager:info("encoding burn type ~p ~p", [Atom, Choices]),
	true = lists:member(Atom, Choices),
	atom_to_binary(Atom, utf8);
encode(Binary, gateway_mode, Choices) ->
	true = lists:member(binary_to_existing_atom(Binary, utf8), Choices),
	Binary.

decode(Bin, gateway_mode, Choices) ->
	Atom = binary_to_existing_atom(Bin, utf8),
	true = lists:member(Atom, Choices),
	Atom.
