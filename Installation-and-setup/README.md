# Installation and Setup

## Dependencies 

### Docker desktop
Make sure that you have Installed Docker desktop and It's running on your computer.
If you don't have docker desktop please download the docker desktop from the official website and follow their instructions (Install Docker Desktop on  [Mac](https://docs.docker.com/desktop/install/mac-install/), [Windows](https://docs.docker.com/desktop/install/windows-install/), or [Linux](https://docs.docker.com/desktop/install/linux-install/).) 

### Devbox
All other software dependencies that we are using in this project are defines in the `devbox.json` and `devbox.lock` file in the root directory.

If you haven't install devbox please install devbox according to their instructions: https://www.jetify.com/devbox/docs/installing_devbox/

Once installed you can run:

```
devbox init 

```

Make sure that the `devbox.json` and `devbox.lock` are in the root directory.

Once you have confirmed that they are in the root directory run 

```
devbox shell

```