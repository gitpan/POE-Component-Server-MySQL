package MyMySQL::OnSteroids;

use MooseX::MethodAttributes::Role;

sub fortune : Regexp('qr{fortune}io') {
   my ($self) = @_;
   
	my $fortune = `fortune`;
	chomp($fortune);

   $self->send_results(['fortune'],[[$fortune]]);

}

1;
