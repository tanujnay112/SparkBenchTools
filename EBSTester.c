#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#define READ_AMT (128*1024*1024)
int main()
{
  int fd = open("/data/cachetraces44", 0);
  int writefd = open("testingFile", O_CREAT|O_WRONLY);
  char *buff = malloc(READ_AMT);
  clock_t begin = clock();
  
  int bytesread = 0;
  while(bytesread < READ_AMT){
    int res = read(fd, buff, READ_AMT);
    if(res < 0){
	printf("HISSSS\n");
	break;
    }
    bytesread += res;
  }
  int written = write(writefd, buff, READ_AMT);
  close(writefd);
  clock_t end = clock();
  printf("%d\n", written);
  if(written < 0){
	printf("%d\n", errno);
  }
  double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
  printf("%d %f\n", read, time_spent);
  return 0;
}
