library('ggplot2')
df <- read.csv('input/lda_running_times.csv', header=FALSE)

plot <- ggplot(data = df, aes(x = V1, y = V2))
plot <- plot + geom_point()
plot <- plot + geom_smooth(method = 'lm', col = 'red')
plot <- plot + labs(x = '# topics', y = 'Running time (s)')
ggsave("lda_running_times.png")
