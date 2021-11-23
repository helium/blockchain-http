-module(bh_transaction_type).

-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3]).

init(Choices, _) -> Choices.

names() ->
    [transaction_type].

encode(Atom, transaction_type, Choices) when is_atom(Atom) ->
    true = lists:member(Atom, Choices),
    atom_to_binary(Atom, utf8);
encode(Binary, transaction_type, Choices) ->
    true = lists:member(binary_to_existing_atom(Binary, utf8), Choices),
    Binary.

decode(Bin, transaction_type, Choices) ->
    Atom = binary_to_existing_atom(Bin, utf8),
    true = lists:member(Atom, Choices),
    Atom.
