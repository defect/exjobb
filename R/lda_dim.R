library('ggplot2')
library('scales')
df <- read.csv('input/lda_dim.csv', header=FALSE, col.names=c('prc', 'k', 'value'))

plot <- ggplot(data = df, aes(x = k, y = value, group = prc, colour = prc))
plot <- plot + geom_point()
plot <- plot + geom_line()

plot <- plot + labs(y = 'Running time (s)', x = 'k', colour = 'Percentage')

ggsave("lda_dim.png")
