library('ggplot2')
library('scales')

df <- read.csv('input/running_times_emr_lda.csv', header=FALSE, col.names=c('nodes', 'k', 'value'))

plot <- ggplot(data = df, aes(x = k, y = value, group = nodes, colour = factor(nodes)))
plot <- plot + geom_point()
plot <- plot + geom_line()
plot <- plot + labs(y = 'Running time (s)', x = 'k', colour = '# nodes')

ggsave("running_times_emr_lda.png")
