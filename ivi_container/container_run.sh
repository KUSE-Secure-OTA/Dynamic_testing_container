podman run --rm -it \
  --name hu_qt5_run \
  --network host --ipc=host --cap-add=NET_RAW \
  -e DISPLAY=$DISPLAY \
  -v /tmp/.X11-unix:/tmp/.X11-unix:rw \
  --device /dev/dri \
  seame_hu_app
