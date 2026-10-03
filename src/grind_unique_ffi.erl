-module(grind_unique_ffi).
-export([sha256/1, random_hex/0]).

%% The uniqueness admission request fingerprint (grind/internal/unique_admission)
%% is hashed in Gleam rather than in SQL, unlike the key digest itself (which
%% stays a PostgreSQL-side sha256(jsonb::text) for jsonb-normalized equality).
sha256(Data) ->
    crypto:hash(sha256, Data).

%% 128 random bits as 32 lowercase hexadecimal characters, for generated
%% submission ids.
random_hex() ->
    binary:encode_hex(crypto:strong_rand_bytes(16), lowercase).
