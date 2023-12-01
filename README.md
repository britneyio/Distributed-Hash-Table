Ise Okhiria
docker build . -f BootstrapDockerfile -t prj5-bootstrap
docker build . -f PeerDockerfile -t prj5-peer
docker build . -f ClientDockerfile -t prj5-client
To run: docker compose -f [testcase file] up