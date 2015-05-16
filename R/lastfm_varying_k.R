library('ggplot2')
df <- read.csv('input/lastfm_varying_k.csv', header=FALSE)

plot <- ggplot(data = df, aes(x = V1, y = V2))
plot <- plot + geom_point()
plot <- plot + geom_smooth(method = 'lm', col = 'red')
plot <- plot + labs(x = 'k', y = 'Running time (s)')
ggsave("lastfm_varying_k.png")
