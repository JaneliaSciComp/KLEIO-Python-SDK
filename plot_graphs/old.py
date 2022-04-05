def plot_file(file_name,save_name= None,items = None,title = None):
    fig = plt.figure()
    fig.set_size_inches(20, 20)
    ax = plt.subplot(111)
    if items == None:
        items = get_header(file_name)

    for item in items:
        if item == 'Remaining_space':
            x,row_i = get_row_normalized(file_name,item)
        else:
            x,row_i = get_row(file_name,item)
#             x = x[1:]
#             row_i = row_i[1:]
        ax.plot(x, row_i, label = item)
        print('got {}'.format(item))
    # naming the x axis
#     plt.xlabel('x - axis')
#     # naming the y axis
#     plt.ylabel('y - axis')
    # giving a title to my graph
    if save_name == None:
        save_name = '{}.png'.format(os.path.basename(file_name))
    if title == None:
        title = os.path.basename(file_name)
    plt.title(title)
    # show a legend on the plot
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
              fancybox=True, shadow=True, ncol=1)
#     plt.savefig(save_name)
    # function to show the plot
    plt.show()

plot_file(full_files_size[3],items= [ 'Used_Size_df'])