with import <nixpkgs> {};

stdenv.mkDerivation rec {
    name = "env";

    #src = ./.;

    # Customizable development requirements
    nativeBuildInputs = [
        automake
        autoconf
        pkg-config
        rdkafka
        gcc
        gprolog
        swi-prolog
    ];

    buildInputs = [
        zlib
        openssl
    ];

}
